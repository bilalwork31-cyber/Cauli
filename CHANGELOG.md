# Changelog

The format follows [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project follows [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## 1.0.0 (2026-08-17)

First release. Version 0.1.0 was never published, so everything below is
relative to the 0.1.0 development series, which some deployments run from
source.

Most of this release is the result of one audit pass over the pre 1.0 tree.
Read the breaking changes first: several of them turn a previously silent
outcome into a raised exception, so code that ran under 0.1.0 can fail under
1.0. That is the intended direction, but it is not a quiet upgrade.

### Breaking changes

These change observable behaviour. The first group makes previously accepted
code raise.

- **Registering two tasks under the same name now raises `ValueError`.**
  Previously the second registration silently replaced the first. The first
  `TaskDef` stayed callable through `.delay()`, but the worker builds its
  registry from the same dict, so calling one function could run the body of
  another. An app with a duplicate registration will now fail at import.
- **`.delay()` and `.apply_async()` validate arguments against the task
  signature and raise `TypeError` on a mismatch.** Previously a wrong keyword
  argument enqueued cleanly and surfaced only on a `.get()` that fire and
  forget callers never make. Validation falls through unchecked when the
  signature cannot be introspected.
- **`result_ttl` and `idemp_ttl` of zero or less now raise `ValueError` at
  `Cauli()` construction, and a negative `max_lateness` raises at schedule
  entry construction.** `result_ttl=0` reads like "disabled" but made Redis
  reject `SET key val EX 0`, so no result key was ever written and
  `AsyncResult.get()` hung forever. A negative `max_lateness` made the entry
  never fire.
- **`AsyncResult.get()` now raises on terminal dead letters where it
  previously blocked forever.** A malformed envelope, an unregistered task
  and an exhausted redelivery limit now write a failure result keyed on the
  recovered task id, with `error.type` naming the cause: `Malformed`,
  `UnregisteredTask` or `RedeliveryLimitExceeded`. The id is recovered from a
  bounded 4096 byte preview; where no id can be recovered there is nothing to
  key a result on and the behaviour is unchanged.
- **`AsyncResult` now raises `TaskFailedError(type="InvalidResult")` for a
  result document that will not decode or is not an object.** Previously a
  raw `msgspec.DecodeError` or an `AttributeError` surfaced, naming neither
  cauli nor the task. `status()` now reports a document missing its `status`
  field the same way `get()` already did, instead of returning `pending`. The
  message for an expired result no longer claims the task is still pending.
- **`cauli.contrib.fastapi` is now `cauli.contrib.sqlalchemy`.** Nothing in
  the module was ever FastAPI specific: it imports no web framework and is
  async SQLAlchemy session lifecycle. The old path is kept as an alias that
  reexports the same objects, so the two import paths share one ContextVar
  and mixing them in one process is safe. `fastapi_app` is the pre rename
  name of `sqlalchemy_app`.

Protocol and worker behaviour:

- **Envelopes with a protocol version above 1 are rejected as malformed.**
  The `v` field was parsed and never checked, so `"v": 99` executed normally.
  The protocol defined no forward compatibility policy, so the conservative
  one was adopted: accept `v <= 1`, route anything higher to the dead letter
  queue with its own log line.
- **The dead letter stream is capped at roughly 1000 entries per queue,
  oldest dropped.** It was written on every malformed, unregistered, expired,
  redelivery limit and exhausted retry entry, with nothing anywhere trimming
  it. Past the cap the oldest dead letters are dropped. That is a deliberate
  data loss tradeoff in exchange for a bound: if the dead letter queue is
  your audit trail, drain or export it before it rotates.
- **A new dead letter reason `not_retryable` is distinct from
  `max_retries`.** Every terminal failure previously dead lettered claiming
  `max_retries`, including failures that never had a retry budget. Reachable
  today through `SerializationError` and a cpu lane registry miss. Code
  matching on the `reason` field needs to handle the new string.
- **A `timeout_ms` of 0, and wrongly shaped `args` or `kwargs`, are dead
  lettered as malformed before the task executes.** A list where an object
  was expected previously reached `fn(*args, **kwargs)`, raised a retryable
  `TypeError`, and burned the full retry schedule with lifecycle hooks on
  each attempt before dead lettering under a misleading reason. A
  `timeout_ms` of 0 made the dispatcher's own timeout elapse first, so the
  job was skipped as a zombie and nothing ever ran.
- **`timeout_ms` is clamped to 24 hours.** A value near `u64::MAX` made the
  recovery loop's idle requirement effectively infinite, so the pending entry
  could never be reclaimed. Clamped rather than rejected, since an extreme
  value plausibly means "effectively no timeout".
- **`--max-envelope-bytes 0` is rejected at startup**, matching how `--batch`
  and `--visibility-timeout` were already validated. It previously dead
  lettered every message.
- **A `backoff_factor` of zero or less is floored at 1.0.** It previously
  collapsed the retry delay to zero for every attempt after the first,
  defeating backoff exactly when a downstream dependency was already
  struggling.
- **A cpu lane registry miss reports `UnregisteredTask` and no longer
  retries.** It previously reported `UnknownTask`, which is not the string
  the protocol documents, and burned the full backoff schedule on something
  that can never succeed.
- **Argument trees nested past the depth limit now fail as
  `SerializationError` and are not retryable.** Both io lanes bucketed the
  only failure mode of the inbound conversion as a retryable
  `WorkerShimError`, so a payload of a few hundred bytes nested too deeply
  burned the whole retry schedule. The outbound direction was already
  correct. The shim also emitted `Unregistered` where the documented string
  is `UnregisteredTask`.
- **Every Redis round trip is now bounded by a response and connection
  timeout, default 5 seconds, configurable with `--redis-timeout`.** Both
  worker connections previously had none, so a Redis that accepted the TCP
  connection but never answered hung fetch, the idempotency claim, the
  delayed mover, crash recovery and every result write indefinitely. The
  Python client now passes `socket_timeout` explicitly instead of inheriting
  whatever the installed `redis-py` happened to default to. A timeout firing on
  a merely slow Redis reaches an existing safe path sooner rather than
  creating a new failure mode, at a cost of one log line and at most one
  visibility timeout of added latency.
- **The idempotency claim TTL is derived from the execution it guards**, now
  `max(idemp_ttl, (timeout_ms + grace) / 1000)`, and the reclaim branch
  refreshes it. Claims can therefore outlive the configured `idemp_ttl`.
- **The mover and beat's claim Lua scripts publish before they destroy.**
  Lua is atomic against other clients but does not roll back on its own
  error, so a failing call left earlier writes committed: the mover removed
  the delayed entry before publishing it, and beat advanced the slot before
  publishing. A mid script error now duplicates rather than loses, which is
  the correct direction under at least once delivery.
- **Completion counters only increment when the Redis write recording the
  outcome succeeded.** The `ok`, `failed`, `dlq` and `expired` counters
  previously credited outcomes whose write had failed, so the stats line
  reported success for work that was about to be redelivered.
- **`cauli-beat` reconciles the code declared schedule only after it holds
  leadership.** A standby running older code previously deleted entries the
  leader had just added, and the leader never restored them, so firings
  stopped permanently until leadership changed. `--once` now declines and
  says so when another instance holds the lease, instead of reporting that it
  did nothing. Its help text no longer states the exactly once guarantee
  without its topology condition.
- **Redis Cluster now fails loudly instead of retrying forever.** The worker
  logs at error level naming CROSSSLOT and the protocol section; beat raises
  `RedisClusterUnsupported`, a type deliberately not a `RedisError` so it
  cannot be mistaken for one. Neither refuses to start, so ready work keeps
  flowing on the worker side. See Known limitations.
- **The stats line gained two fields**, `async_rejected` and `cpu_backlog`.
  Anything parsing that line by position needs updating.

### Fixed

#### Silent data corruption and lost work

- **A large integer argument arrived in Python as a lossy float.** `args` and
  `kwargs` were a bare JSON value and serde_json silently degrades any
  integer literal outside the i64 and u64 ranges to `f64` at parse time,
  before any cauli code runs. A `uuid.uuid4().int` argument,
  338958331192819208857724424333372550912, arrived as
  3.389583311928192e+38: wrong value, wrong type, no error, no dead letter
  and no log line. 2^64 was the first value affected; 2^63 and `u64::MAX`
  survived. The parser now keeps the exact source digits and rebuilds an out
  of range integer literal as a Python int, which round trips exactly because
  Python ints are unbounded. A genuine float still takes the float path.
- **A healthy cpu child was killed for being busy, at default settings.** The
  worker stages prefetched requests into a child's socket buffer whether or
  not the child is draining, and treated any write stalled past 5 seconds as
  evidence of a wedged child, then sent SIGKILL. A child legitimately busy on
  a task longer than 5 seconds was killed mid execution and its prefetched
  siblings were lost, recurring identically on the replacement child. A
  stalled write is now judged against the child's own progress rather than a
  flat clock.
- **A mid script Lua error lost the entry entirely**, gone from the sorted
  set and never published to the stream. Reproduced with a wrong type error;
  an out of memory error under the default `maxmemory-policy noeviction`
  reaches the same path. Both scripts now publish before consuming.
- **An idempotency claim could expire while the execution it guarded was
  still running**, letting a second attempt claim the work as fresh and run
  concurrently with the first: exactly the duplicate the key exists to
  prevent.
- **A standby `cauli-beat` reaped the leader's schedule**, freezing firings
  permanently until leadership changed. A rolling deploy hazard, reproduced
  with two real beat processes.
- **A write error on an already dead cpu child was treated as a wedge**, so
  the worker sent SIGKILL to a pid the kernel may already have recycled to an
  unrelated process. It now takes the same path as any other observed exit.
- **A periodic slot was permanently skipped in zones with a two hour spring
  forward.** Wall clock order and instant order invert inside a two hour gap,
  so the scheduler answered with a later instant and never returned for the
  earlier real one. Found by walking real UTC minutes as ground truth across
  14 zones, 16 expressions and 25 transition days; 3 lost slots before, 0
  after, all in `Antarctica/Troll`.

#### Tasks that blocked forever

- **Terminal dead letters wrote no result key**, so `AsyncResult.get()` with
  no timeout polled a key that would never appear. This was the single root
  cause behind three separately reported hangs: a malformed envelope with a
  recoverable id, a `result_ttl` of 0, and an oversize `.delay()`. Fixed once
  at the dead letter path rather than three times at the symptoms.
- **Every Redis round trip could hang forever**, and not only at shutdown.
  The delayed mover and the crash recovery loop hang the same way in steady
  state, silently, and recovery is the code whose whole job is to help when
  something else has already broken. The blocking wait on `XREADGROUP` is a
  server side wait, not a client deadline, and TCP keepalive does not help a
  paused peer whose own kernel still answers probes. The fix works at the
  connection config so the client crate's existing but dormant reconnect path
  activates; verified by freezing a Redis and watching the same connection
  recover after it was thawed.
- **A `timeout_ms` near the integer ceiling made a pending entry permanently
  unreclaimable**, and the dispatching worker held its admission permit
  effectively forever, burning the full drain timeout at shutdown.

#### Unbounded growth

- **One blocking call inside an `async def` grew a queue forever while the
  metric built to catch it stayed flat.** A wedged loop thread never returns
  to callback processing, so the per loop pending queue kept accumulating
  live Python arguments with no ceiling, while `pending_async` reported the
  Rust side map, which self heals on its own timer. That divergence, RSS
  climbing with the canary flat, is the best available explanation for the
  slow memory growth question that was previously open. Each loop's queue is
  now capped at 4096 submissions, rejections are counted in `async_rejected`,
  and one warning fires the first time a loop reaches its cap.
- **The dead letter stream grew without bound.** It was written on every
  terminal failure with no length limit, and nothing in the worker, the
  client or the docs ever trimmed it. Any sustained trickle of failures on a
  long lived worker grew it until Redis ran out of memory, taking down every
  queue in the deployment rather than only the failing one.
- **The sync pool spawned a replacement thread on every hard timeout with no
  ceiling**, at a measured rate of roughly 4 threads per second under a
  systemically hanging dependency, each costing a pinned interpreter thread
  state, an 8 MB stack reservation and whatever its frame held. Capped at
  four times `--io-threads`.
- **Exception text written into result keys and dead letter entries was
  uncapped.** A 50,000 character exception produced a 50,012 byte string in
  Redis. Capped at 8192 characters, matching the caps the two Python lanes
  already had.

#### Credential exposure

- **A configuration parse failure printed the whole config, including the
  Redis password, in an error line.** Triggered by ordinary misconfiguration,
  such as a negative `result_ttl`. The raw config no longer enters the
  message at all; the serde error's own position and type still surface, so
  nothing useful for debugging was lost.
- **Both Redis URL maskers split user info at the first at sign** where the
  URL parsers on both sides split at the last, so a password containing an at
  sign leaked its tail through masking. Confirmed by execution, in Python and
  in Rust.
- **Neither masker touched the `?password=` query form**, which has no at
  sign at all, and returned it verbatim. `username=` is now masked too. The
  fix scopes the split to the URL authority, so an at sign inside a later
  query value is not mistaken for a user info delimiter.
- **An unmatched cpu child response was logged with its payload**, carrying
  the task result and error message into the worker log. It now logs the
  response id and length only.

#### Broken, but invisible

- **A full cpu backlog silently paused fetching for every lane, io
  included.** The fetch loop cannot know a message's lane before parsing it,
  so backpressure across lanes is correct in principle; what was wrong was
  that it was invisible, with no counter, no stats field and no log line. Now
  reported as `cpu_backlog` with one warning when the backlog forms and one
  when it clears. The regression test drives a real fork server past its
  backlog and asserts the io lane really does stall, rather than asserting a
  counter moved.
- **A main thread panic bypassed the worker's own exit path**, letting the C
  runtime run atexit handlers while Python threads were still live. That is
  the same mechanism as the incident the hardened exit path was written to
  prevent, reached by a route it did not cover. Caught deterministically by
  observing whether atexit ran, not by hunting for corruption afterwards.
- **`cauli-beat` dropped due slots with no log line at any level**, and
  swallowed a coalesced backlog without saying how large it was: six hours
  down on a 60 second cadence reported lateness and never mentioned the 359
  slots that would not fire. Both now log at warning level, and the count is
  omitted rather than guessed when it cannot be known exactly.
- **A pre epoch system clock silently made every worker local timestamp read
  as zero.** The value is unchanged, since it is self consistent across
  dispatch and the mover, but it now warns once instead of degrading in
  silence.
- **Two second to millisecond conversions were non saturating multiplies**
  with overflow checks off in release builds, and the `--print-plan` totals
  line wrapped the same way, printing "2 io tasks in flight" for an
  enormous input. All now saturate, matching the convention used elsewhere in
  the worker.
- **Redis Cluster failures read as ordinary transient blips.** The mover
  warned and retried every 250 ms forever while every delayed and retried
  task sat in the sorted set permanently, and beat's seed call had no handler
  at all, so no periodic task ever fired. Both silent.
- **A periodic entry naming an unregistered task was accepted silently** and
  dead lettered forever with no signal. `Cauli.check_periodic_tasks()` now
  runs once at `cauli-beat` startup, after the app module has finished
  importing, and logs the offending entry without aborting the process.
- **An unconfigured `Cauli()` used `redis://localhost:6379/0` with no
  signal.** On a box with an unrelated Redis on that port, tasks vanished
  into the wrong instance with no error. The default is unchanged; it now
  warns when it is applied.
- **A cpu child killed by a signal was indistinguishable from any other
  death**, so a segfault, an out of memory kill and the worker's own hard
  timeout all looked identical to an operator. The signal number is now
  logged.
- **A child that forked and then died immediately had no backoff**, measured
  at 14.4 fork and crash cycles per second under a bad deploy. It now backs
  off from 100 ms to 2 seconds, mirroring the backoff the refused path
  already had.
- **The `-c` derivation was documented with plain division where the code
  uses ceiling division**, so `-c 65` resolves to 2 processes where a reader
  computing by hand predicts 1. The direction is always more resources than
  predicted, never fewer, but the same document carries the database
  connection count formula. Also corrected: `--procs N` alone leaves
  `--io-threads` and `--io-concurrency` at their flat per process defaults,
  so fleet wide totals multiply rather than stay fixed.

### Added

- **`cauli.contrib.sqlalchemy`**: one async SQLAlchemy session per task,
  opened and unconditionally closed around the task body through a
  `ContextVar`, with the engine built once and disposed at fork. Public
  surface is `sqlalchemy_app()`, `install_sqlalchemy_session()` and
  `get_session()`. Verified against real Postgres: 5000 tasks peaked at 15
  backends, exactly SQLAlchemy's own `pool_size` plus `max_overflow` default,
  and settled back to 5. It ships with an end to end integration test against
  a real worker and a real database, matching what the Django integration
  already had. Deliberately not included: any commit behaviour, cpu lane
  support, or an on commit enqueue helper.
- **`--redis-timeout`**, default 5 seconds, setting both the response and the
  connection timeout on both worker Redis connections. Exposed as a flag
  rather than hardcoded because the correct value depends on a deployment's
  real Redis tail latency: below roughly 1 second, ordinary fork, fsync and
  network jitter risks false trips; past roughly half the visibility timeout
  it buys nothing.
- **Two observability fields in the stats line.** `async_rejected` counts
  submissions rejected because a loop's queue is at its cap, which means that
  loop is wedged and the process needs a restart. `cpu_backlog` reports the
  cpu pool's overflow depth, with edge triggered warnings when it fills and
  when it clears, since a full backlog pauses fetching for every lane.
- **A startup warning when `idemp_ttl` is shorter than a registered task's
  `timeout_ms`**, in the style of the existing visibility timeout warning.
- **`Cauli.check_periodic_tasks()`**, validating that every declared periodic
  entry names a registered task. Called once at `cauli-beat` startup rather
  than at declaration time, since an entry may legitimately name a task
  registered later in the same module.
- **Envelope integer fields accept exponent and float form** when the value
  is an exact whole number in range, since the protocol invites third party
  codecs and several emit large integers as `1.7e12`. A fractional value, a
  non finite value or one out of range is still malformed, never rounded or
  saturated to fit.
- **Documentation**: the database connection count formula for the Django
  integration and the pooler it implies; what `rss_mb` does and does not
  cover and the only bound on cpu child memory; which stats fields mean
  something is already broken; the Redis Cluster, persistence, deploy order
  and client blocking caveats in the README; and protocol sections for the
  new result key writes, the version policy, the dead letter cap,
  `not_retryable`, the accepted integer forms, and the corrected reasoning
  behind beat's exactly once guarantee.
- **Tests** for the two highest stakes paths that had none: crash redelivery
  resolving to a reclaim rather than a duplicate, and the redelivery limit
  dead letter path end to end.

### Known limitations

- **Redis Cluster is not supported.** The worker links no cluster protocol at
  all, so it never follows a MOVED redirect and ordinary operations fail
  against a real multi node cluster, not only the delayed and periodic paths.
  Those fail for a second reason as well: the mover's two keys and beat's
  seed keys never share a hash slot, so Cluster rejects every call with
  CROSSSLOT. Both failures are now loud. Standalone and Sentinel are the
  supported topologies. Hash tags would fix the CROSSSLOT half but would
  change the key naming scheme, and beat's claim atomicity cannot be fixed
  without a single global hash tag, which would pin every key to one slot and
  remove the only reason to run Cluster.
- **Redis must be persistent and must never come back empty.** The stream,
  its consumer group, the pending entries list and the delayed sorted set are
  the only copy of accepted work. The consumer group is created at worker
  startup only, so a Redis that restarts empty leaves workers alive but deaf:
  they warn on each fetch and consume nothing until they are restarted. That
  is the ElastiCache default, and it is also what an out of memory kill or a
  restore from backup looks like.
- **A wedged event loop needs a process restart.** One blocking call inside
  an `async def` ends that process's async throughput for the life of the
  process. Nothing detects a wedged loop thread, nothing replaces it, and at
  the default `--io-loops 1` that is the entire async lane of that process.
  `async_rejected` is the signal, and it only starts moving once 4096
  submissions have accumulated behind the wedge.
- **A hard timed out sync thread and its database connection are lost until
  restart.** CPython has no safe way to stop a running thread. The task is
  marked failed and a replacement thread is spawned so capacity is preserved,
  but the original call runs on, its cleanup never executes, and the database
  connection it holds is held for the life of the process. `sync_abandoned`
  counts each occurrence. Use `kind="cpu"` for work that must be killable.
- **Beat can refire one slot after a failover with replication lag.** The
  compare and set that makes each slot fire once is only as atomic as the
  node holding it, and no client side code fixes asynchronous replication.
  This applies to the Sentinel path too, and `idempotent=True` does not
  protect against it, because the idempotency key sits in the same lost write
  window. The guarantee is exactly once per surviving Redis dataset.
- **`TimeoutError` means three different things.** A caller timeout from
  `.get(timeout=)` raises the Python builtin, while a worker enforced timeout
  and a task's own raised `TimeoutError` both arrive as
  `TaskFailedError(type="TimeoutError")`. So `except TimeoutError:` around a
  `.get()` catches only the first case and silently misses the worker
  enforced one. Renaming the worker's sentinel is a public API decision that
  was deliberately not taken during the audit, and two of the three cases
  would remain indistinguishable by type even after a rename.
- **The worker uses its local clock while beat reads Redis `TIME`.** An NTP
  step while computing a retry time durably writes a far future score into
  the delayed set and strands the task with no self healing. Clock skew
  across a fleet desynchronises retry and expiry timing, and since every
  worker runs the mover, the fastest clock in the fleet decides when delayed
  work fires. Keep workers on NTP.
- **The stats line carries no latency telemetry.** All fields are counts and
  gauges. A degradation that preserves throughput while inflating latency is
  invisible: an operator sees `ok` still climbing and nothing else. Precision
  measurement lives in `bench/`.
- **cpu child memory is invisible and recycling is off by default.** `rss_mb`
  covers the worker process only; forked children are separate processes and
  are never summed into it. A child was measured holding 331.8 MB while
  `rss_mb` read 35. `--cpu-max-tasks-per-child`, default 0 meaning never
  recycle, is the only bound on a child's memory: cauli sets no rlimit and no
  cgroup. Set it to a nonzero value in production.
- **A background task that outlives a cauli task resurrects its SQLAlchemy
  session.** `AsyncSession` transparently reopens after `close()`, so a
  leaked reference does database work the session lifecycle cannot see and
  checks out a connection nothing will ever close. This is not preventable in
  the integration module; it is a contract on task code.
- **A soft timeout does not stop the database query server side.** The client
  times out and the connection pool recovers correctly, but the backend stays
  active for the query's natural duration, holding locks, invisible to cauli.
  Inherent to asyncio cancellation over psycopg, which sends no server side
  cancel.
- **`.delay()` is synchronous Redis I/O.** Inside an async handler it blocks
  the event loop, for up to the client's 5 second socket timeout when Redis
  degrades. There is no async enqueue API.
- **No Python `atexit` handler ever runs.** The worker leaves through an
  immediate process exit, deliberately, because running C library atexit
  handlers while Python threads are live is what caused the corruption
  incident that shaped this design: one cleanup path aborted 39 percent of
  shutdowns at 80 io threads before the change. The practical cost is that
  exit time flushes do not happen, so Sentry and OpenTelemetry users must
  flush explicitly, and buffered task output at shutdown is lost.
- **A stalled fetch loop delays the start of the drain.** The worker waits
  for the fetch loop to return before it computes the drain deadline. The
  Redis response timeout shrinks that window sharply, since the loop can no
  longer hang forever, but does not close it. Separately, a Redis outage
  during shutdown costs the full `--drain-timeout` every time, measured at 20
  of 20 runs: worth knowing when sizing a container's termination grace
  period.
- **conda environments cannot run the worker.** The wheel installs and the
  worker then fails before it starts, because the loader cannot find that
  environment's libpython. `pyenv` needs `PYTHON_CONFIGURE_OPTS="--enable-shared"`.
  System CPython, Docker `python` images, venv and virtualenv on top of
  those, distro packages and `actions/setup-python` all work. The worker is
  Linux only, including from source, because it arms `PR_SET_PDEATHSIG`
  unconditionally.
- **Memory was measured on the happy path only, so far.** A 40 minute soak at
  216,000 tasks reached genuinely flat, with post warmup growth of 0.43 bytes
  per task, at or below page quantization and therefore an upper bound rather
  than a measured leak rate. That run never exercised retries, dead letters
  or expiry. A longer soak against a deliberately failing workload is the
  routine that continues after this release.

### Upgrade notes

**Upgrade workers before producers.** A worker that does not recognise a task
name consumes the message and dead letters it terminally. That was already
true before 1.0; what changed is that it now also writes an
`UnregisteredTask` failure result, so the caller stops waiting and receives a
definitive error rather than blocking. Either way the task does not survive
the rollout to be picked up by a newer worker, so in a rolling deploy every
worker must be running 1.0 and must know the new task name before anything
enqueues it.

Before deploying:

- Check for duplicate task names. A second registration under the same name
  now raises at import, so an app that starts today may refuse to start.
- Check every `Cauli()` for `result_ttl` or `idemp_ttl` of 0, and every
  schedule entry for a negative `max_lateness`. All three now raise.
- Check `.delay()` and `.apply_async()` call sites. A keyword argument that
  does not match the task signature now raises `TypeError` at the call site
  instead of failing later.
- Move imports from `cauli.contrib.fastapi` to `cauli.contrib.sqlalchemy`.
  The old path still works.
- If you match on the dead letter `reason` field, handle `not_retryable`
  alongside `max_retries`.
- If you parse the stats line, it gained `async_rejected` and `cpu_backlog`.
- If your Redis tail latency exceeds 5 seconds under load, raise
  `--redis-timeout`. A false trip costs one log line and at most one
  visibility timeout of added latency on that task, never data loss.
- If the dead letter queue is your audit trail, arrange to drain or export it:
  it now holds roughly the most recent 1000 entries per queue.
