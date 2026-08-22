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
- **`crontab()` is keyword only past `hour`.** cauli's field order is
  `cron(8)`'s: minute, hour, day_of_month, month, day_of_week. Celery's third
  and fourth positional arguments are the other way round, so a copied
  `crontab(0, 4, 3)` used to build a silently different schedule. Three or more
  positional arguments now raise `TypeError`. `crontab(0, 4)` and every keyword
  form are unchanged. Celery's `month_of_year=` is accepted only as a trap: it
  raises a `TypeError` naming cauli's `month=` and the swapped positions.
  Related and unchanged: when both `day_of_month` and `day_of_week` are
  restricted, cauli ORs them the way `cron(8)` does. Celery ANDs them.
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
  oldest dropped, and expires 7 days after the last dead letter written to
  it.** It was written on every malformed, unregistered, expired, redelivery
  limit and exhausted retry entry, with nothing anywhere trimming it. Past
  the cap the oldest dead letters are dropped. The 7 day `EXPIRE` is
  refreshed on every write, so a queue that keeps failing keeps its stream
  and a queue that stopped failing releases the memory instead of holding
  task arguments in Redis forever. Both bounds are deliberate data loss
  tradeoffs: if the dead letter queue is your audit trail, drain or export it
  before it rotates or expires.
- **BREAKING: idempotency keys are derived with SHA-256, not 64 bit
  FNV-1a.** The Redis key is now `cauli:idemp:{first 128 bits of SHA-256 of
  the key, hex}`, a fixed 32 hex characters. At 64 bits a collision was
  silent task loss: the colliding task took the Duplicate branch, was acked,
  XDELed, handed someone else's result and never ran. FNV-1a is also
  invertible, so a caller who controls one key could suppress another
  tenant's task deliberately. No new dependency: the digest is implemented in
  crate, `sha2` is not in `Cargo.toml`. Keys minted by an older build do not
  match new ones, so a claim in flight when a worker is replaced is invisible
  to the new worker and a task carrying an `idempotency_key` can execute
  twice during a rolling upgrade. See the upgrade notes.
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
- **A worker enforced time limit is now `"TimeLimitExceeded"`, not
  `"TimeoutError"`.** Three different things used to share that one spelling,
  two of them indistinguishable. A caller giving up still raises the Python
  builtin locally from `.get(timeout=)` and never writes a result document; a
  worker killing the task at its limit is now
  `TaskFailedError(type="TimeLimitExceeded", origin="worker")`; a task
  raising `TimeoutError` itself stays
  `TaskFailedError(type="TimeoutError", origin="task")`. The new name is
  symmetric with the `SoftTimeLimitExceeded` that already existed and no
  longer shadows a builtin, so `except TimeoutError:` around a `.get()` no
  longer silently misses the worker enforced case. Anything matching the old
  string needs updating. See PROTOCOL section 8.2, and Known limitations for
  the one async case that stays conflated.

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
- **A completion acks and deletes its stream entry in one transaction.** The
  two commands used to travel as a bare pair, so a connection that died
  between them left the entry acked but still resident in the stream:
  invisible to every pending entries scan, invisible to the backlog scan,
  reaped by nothing, and there forever. `add_ack_del` now emits MULTI, XACK,
  XDEL, EXEC. A tear now costs the EXEC instead, Redis discards a
  transaction whose client disconnects before EXEC, and the entry stays
  pending for the section 4.4 recovery loop to redeliver. That trades a
  permanent leak for a redelivery, which at least once delivery already
  allows. The MULTI wraps only that pair and never the surrounding pipeline:
  both commands address the one key `cauli:q:{queue}`, so the transaction
  stays in a single hash slot and anyone running with
  `CAULI_ALLOW_REDIS_CLUSTER=1` keeps working. Cost is two small commands on
  a round trip that already happens. No extra round trip, no Lua, no wire
  change.

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
- **Every worker restart left a dead consumer behind in the group forever.**
  Each process joins under its own consumer name, and nothing ever removed
  the old ones, so a fleet on a daily rolling deploy accumulated one entry
  per process per deploy in `XINFO CONSUMERS` and in Redis memory. The
  recovery loop now reaps them once every 20 recovery ticks, strictly limited
  to a consumer with zero pending entries, that is not this process, and that
  has been idle for at least the greater of 4 visibility timeouts and 10
  minutes. Zero pending is a hard rule, never a heuristic: a consumer holding
  work is never deleted, and a `XGROUP DELCONSUMER` reply reporting a nonzero
  pending count is logged at warning level.

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
- **A worker held back by a full cpu backlog no longer spins a core.** The
  fetch loop and the recovery loop wait on a gate that is a conjunction: free
  io permits and a cpu backlog of zero. Making that wait event driven on the
  semaphore fixed a 25 ms latency floor but introduced a live spin, because
  with io permits free the acquire was granted instantly, the loop went round,
  hit the same closed gate, and burned CPU at full speed for the whole
  duration of the backlog. The wait now branches: when the io half is already
  open the blocker is the cpu backlog, which nothing signals, so the gate
  checks again every 25 ms as it did before; when io permits are the blocker it
  stays event driven and a freed slot wakes the loop immediately. No permits
  are held while waiting. Idle latency is unchanged.

### Added

- **`error.origin` on the result document**, valued `task` or `worker`, so a
  caller can tell an exception that came out of its own code from one cauli
  synthesized. Additive: a client reading an older result simply finds it
  absent. Exposed as `TaskFailedError.origin`. This is what makes the
  `TimeLimitExceeded` rename above usable, since a task raising its own
  `TimeoutError` and the worker enforcing its limit are otherwise
  indistinguishable.

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

### Pre release audit fixes

1.0.0 was never tagged before this pass. A five lens review of the tree found
5 blockers and 19 high findings; everything below landed in response, so a
reader comparing 1.0.0 against notes written earlier in the series will find
these behaviours changed under the same version number.

**Connectivity and the client surface**

- **`rediss://` works.** The worker previously linked no TLS crate at all, so
  every TLS only managed Redis (Upstash, Azure Cache, ElastiCache with
  encryption in transit) failed at `Client::open` and exited 1 while the Python
  client happily kept enqueueing into it. rustls is compiled in and trusts the
  platform certificate store; no build flag, no OpenSSL headers.
- **`AsyncCauli` and the awaitable enqueue path.** `await task.adelay(...)`,
  `aapply_async`, `await result.aget(...)` and `astatus`, on `redis.asyncio`.
  The envelope is identical to the blocking path's. `AsyncCauli` subclasses
  `Cauli`, so the blocking methods keep working on the same app.
- **`Cauli(redis_client=...)`** takes a ready client or a zero argument
  factory, which is the supported way to reach a Sentinel set from the Python
  client and from `cauli-beat`. The Rust worker connects by URL and never looks
  a master up again, so the Sentinel claim now says exactly that instead of
  claiming a supported topology.
- **`cauli.current_task()`** exposes the running task's id, retry count, retry
  ceiling, name and queue on all three lanes, backed by a ContextVar so it is
  correct on the sync thread pool and on the asyncio lanes alike. It returns
  None outside a worker. Tasks previously had no way to log their own id or
  tell they were on the last attempt.
- **A task defined in a `__main__` script is registered under its importable
  module name**, resolved from `__main__.__file__`. It used to be stamped
  `__main__.<task>`, which no worker registry can match, so the first ten
  minutes of the quickstart ended in a terminal dead letter.
- **A `countdown`, `eta` or beat slot later than the envelope's own deadline
  raises `ValueError` at enqueue.** `queue_ttl` is measured from enqueue, so
  `Cauli(queue_ttl=300)` plus `countdown=600` used to publish cleanly, wait out
  its delay, and then be discarded unrun with no exception and no log.
- **`delay_on_commit` validates before it defers.** The signature check and a
  JSON encode dry run now run at the call site, inside the atomic block, so a
  bad call rolls the transaction back instead of raising at COMMIT with the row
  already written. Both docstrings also state the one thing that surprises
  every Django user: under `django.test.TestCase` nothing is ever enqueued,
  because the test is wrapped in an atomic block that always rolls back. Use
  `self.captureOnCommitCallbacks(execute=True)` or `TransactionTestCase`.
- **The worker refuses to start against Redis Cluster.** The check existed but
  asked the wrong command. `CLUSTER INFO` carries no `cluster_enabled` field on
  a cluster node and is refused outright by a standalone, so the probe could
  never prove a cluster and the refusal never fired. It now sends
  `INFO cluster`, which is the only reply carrying `cluster_enabled:0` or
  `cluster_enabled:1`, and exits 1 before touching a consumer group.
  `CAULI_ALLOW_REDIS_CLUSTER=1` starts anyway and accepts the loss.
- **`AsyncResult.status` reads as an attribute and as a call.** `r.status ==
  "success"`, the Celery spelling, used to compare a bound method against a
  string and was quietly False forever. `status` is now a property returning a
  `str` subclass whose `__call__` returns itself, so both spellings work and
  `r.status()` still costs exactly one Redis read.
- **A deduplicated caller can reach the task that actually ran.**
  `AsyncResult.claimant_id` carries the claiming task id once a `duplicate`
  resolves, and `AsyncResult.claimant()` returns a result handle for it. Both
  were on the wire and thrown away. `claimant_id` stays `None` for the section
  4.5 race where the worker itself could not read the claim holder.
- **`Cauli(max_envelope_bytes=...)`, default 1048576, refuses an oversize
  enqueue at the call site.** The limit matches the worker's
  `--max-envelope-bytes` default. There was no client side check at all: an
  oversize envelope published cleanly, the worker dead lettered it as
  malformed, and a payload past the worker's 4096 byte id recovery window left
  no result key for `get()` to ever return. Nothing is written when the check
  fires.
- **`Cauli.enqueue_many()` and `AsyncCauli.aenqueue_many()`** publish N tasks
  in one pipelined round trip instead of N. Each element is a task, or a
  `(task, args, kwargs, options)` tuple with the trailing parts optional. The
  whole batch is validated and encoded before the first write, so one bad or
  oversize call aborts the batch with nothing published. The pipeline is
  explicitly not a transaction.
- **`tzdata` is installed on every platform, not only Windows.** Alpine and
  distroless images are `sys_platform == "linux"` and also ship no
  `/usr/share/zoneinfo`, so the marker dropped the wheel exactly where
  `zoneinfo` has no database and `crontab(timezone="Europe/Berlin")` raised
  `ValueError` at app import. Costs about 450 KB everywhere.
- **`@app.task` no longer erases the decorated function for a type checker.**
  The package ships `py.typed`, but the single union return type made every
  decorated name resolve to `TaskDef | Callable`, so `.delay()` did not resolve
  on it. Two `@overload`s fix both decorator spellings. Runtime is unchanged.
- **A cpu child response that failed to write is logged.** The write error was
  swallowed by a bare `except BaseException: pass`, so a broken pipe to the
  parent left no trace anywhere.

**Timeouts and lane behaviour**

- **`soft_timeout` on an `async def` task raises `SoftTimeLimitExceeded`.** It
  used to collapse to the smaller of the two limits and report
  `TimeLimitExceeded`, so a Celery migrant porting `soft_time_limit=30,
  time_limit=300` lost 90 percent of the budget and had a cleanup handler that
  could never fire. The async lane signals the soft mark by cancelling the
  task, so a body observes `asyncio.CancelledError` and its `finally` blocks
  run; the reported failure is `SoftTimeLimitExceeded` on all three lanes.
- **A sync task no longer spends its `timeout` waiting for a pool thread.**
  The admission gate and the thread pool are sized independently, so a job
  could sit in the pool's queue for the whole hard timeout and then be reported
  as a wedged thread that never existed. The timeout now starts when a thread
  commits to the job; a job that never reaches one fails as a retryable
  `WorkerLost` naming that, and does not spawn a replacement thread.

- **An `async def` task with `kind="cpu"` runs its before and after hooks on
  its own event loop**, and an awaitable a hook returns is awaited there. They
  used to run on the plain calling thread, so a hook that branches on
  `asyncio.get_running_loop()` took the wrong branch. That is exactly what
  `cauli.contrib.django`'s connection hooks do, so `close_old_connections` ran
  on a thread that did not hold the connection it was closing. After hooks now
  fire on every outcome, including a raise. Sync cpu tasks and both io lanes
  are untouched.

**Redelivery races, each of which could run a task twice or dead letter one
that never ran**

- **`XREADGROUP COUNT` is divided across streams.** Redis applies COUNT per
  stream, so `-Q high,default,bulk` fetched three entries for one free slot and
  parked two with their idle clocks running.
- **The recovery page is bounded by free permits** instead of always claiming
  a fixed 128 on a gate that proved one free slot.
- **A cpu task takes an admission slot the fetch loop can see**, and a worker
  skips reclaiming a stream entry it is still holding itself.

**Operational**

- **`--redis-timeout` is forwarded to supervised worker processes.** It was
  the one runtime flag `child_argv` omitted, and `-c 200` takes the supervisor
  path without anyone typing `--procs`, so an operator who raised it after an
  incident got no effect anywhere and no log line. A test now enumerates the
  parser's own arguments and fails unless each one is forwarded or listed with
  a reason.
- **`--redis-url` no longer reaches a child's argv.** The supervisor passes it
  through the inherited `CAULI_REDIS_URL` instead, so a Redis password is not
  in `ps aux` and in one `/proc/<pid>/cmdline` per process.
- **The delayed and retry sweep is no longer capped at 512 per second per
  queue per process.** It repeats within a tick until a queue returns a short
  page. New `--mover-interval` (250 ms) and `--mover-limit` (128) flags.
- **`--python` defaults to the embedded interpreter's own `sys.executable`.**
  The old default was the bare string `python3` resolved through PATH, so a
  systemd unit or a Docker CMD that never activates the venv found an
  interpreter with no `cauli` package. That failure took the whole worker
  offline, not only the cpu lane, at warn level.
- **`--redis-timeout 1` no longer causes a reconnect storm.** The fetch loop
  derives its `XREADGROUP BLOCK` window from the timeout, half the client
  deadline capped at 1000 ms with a 50 ms floor, and logs the shortened window.
- **The async lane's reclaim threshold clears its own backstop.** The two were
  the same expression measured from different instants, leaving the async lane
  no reclaim margin at all.
- **`oldest_ms` cannot be poisoned by an orphan.** It reads the pending entries
  list and the undelivered tail rather than a plain `XRANGE`, so an XACK whose
  XDEL never landed no longer reports a phantom age that grows forever and
  survives every restart.
- **Every dead letter reason is visible at the default log level.** Four arms
  were silent or at `debug!`: a missing `e` field, the unparseable catch all,
  an unregistered task and an expired task. A producer and worker version skew
  that dead lettered a whole queue used to produce no log output at all.
- **The stats line carries `pid=`, `host=` and `duplicate=`,** and `retried`
  is incremented only when the retry write succeeded. During a Redis brownout
  `retried` used to climb at full rate while nothing was scheduled. Section 7
  of PROTOCOL.md is updated: those three keys are part of the frozen 1.x key
  set.

**Documentation and repository**

- Eight figures marked as measured in `docs/CONFIGURATION.md` came from a
  campaign whose harness is not in this repository and cannot be rerun by
  anyone, including the maintainer. They are removed, not restated. The
  guidance they supported is kept as design rationale and marked as such.
- Three more unbacked figures were carried by "Known limitations" itself and
  are gone the same way: a 39 percent shutdown abort rate, a Redis outage
  drain cost "measured at 20 of 20 runs", and a 40 minute soak reporting 0.43
  bytes of growth per task across 216,000 tasks. None of the three appears in
  `bench/`, and on the soak the tracked suite says the opposite: its only soak
  was killed by an environment outage and never produced a verdict. The memory
  entry now says that plainly instead of publishing a number over it. The rule
  going forward is in CONTRIBUTING.md: no figure in any document without a
  harness in `bench/` that reproduces it.
- `--io-concurrency` defaults to 256 while `bench/RESULTS.md` reports a stall
  above 104 per process. Both ship in this release. `docs/CONFIGURATION.md`
  now states the disagreement, says the stall is unattributed and may be the
  harness's own Redis connection count, and tells you to treat anything above
  128 slots per process as untested.
- Five entries in "Known limitations" described behaviour the shipped code
  contradicted: NOGROUP self healing, the wedge watchdog, the clock, latency
  telemetry and cpu child recycling. All five are rewritten against the code.
- Internal audit records (`AUDIT.md`, `FIXES.md`, `RESUME.md`,
  `docs/AUDIT_LOG.md`) are no longer tracked. They named unpushed branches and
  interim verdicts that read as the project's current position.
- `scripts/check_versions.py` reads README.md's Status section as a fifth
  version source. Four artifacts shipped 1.0.0 marked Production/Stable while
  the landing page said v0.1 and CI stayed green, because nothing read it.
- `docs/decisions/` is reframed as historical design notes with a verified
  status line per document, instead of nine documents all stamped as not
  implemented while several of them had shipped. Two status lines were still
  wrong after that pass and are corrected here: `redis-cluster.md` claimed the
  startup refusal did not exist, and `delivery-guarantee.md` claimed
  `AsyncResult` still discarded the claimant id. Both had shipped.
- `docs/MIGRATING-FROM-CELERY.md` is new. README promised Celery migration
  notes and pointed at CHANGELOG's upgrade notes, which are about upgrading
  cauli, not about leaving Celery. The new document is a mapping table, the
  eight divergences that bite silently, and the list of Celery features cauli
  does not have.
- `py/README.md`, the page PyPI renders, stated neither that the worker is
  Linux only nor which CPython versions its wheels cover, and listed four of
  the five statuses `status()` returns. Both are fixed and both are pinned by
  tests.

**Still open, and listed here rather than found later**

- No CLI exists for the dead letter queue, queue depth or the delayed set.
  `cauli-beat` is the only console script, so Celery's `inspect`, `purge` and
  `control` have no equivalent.
- The delayed and retry sweep has no published throughput figure, and `bench/`
  has no retry rate lane.
- Nothing XTRIMs `cauli:q:{queue}`. The ack and the delete are one
  transaction now, so no new orphan can be created, but an orphan left in a
  live stream by a pre 1.0 build stays there. A periodic sweep was considered
  and rejected: MAXLEN and MINID cannot tell an orphan from a legitimately
  pending or undelivered entry, so a sweep aggressive enough to reap orphans
  can delete live work.
- `AsyncResult.get()` and `aget()` poll `GET cauli:result:{id}` every 50ms by
  default. There is no push notification, so a result wait carries a 25ms mean
  floor and 20 reads per second per waiter. `poll_interval` is tunable per
  call. Replacing the poll with pub/sub is a wire change and is not in 1.x.
- `--io-concurrency` defaults to 256 while the only slot sweep in `bench/`
  stalls above 104 per process. The stall is unattributed: the same harness
  opens one Redis connection per concurrent caller, so it may have exhausted
  the server's client slots rather than found a limit in cauli. Treat anything
  above 128 slots per process as untested. `docs/CONFIGURATION.md` carries the
  full note.
- `cauli-beat` has no envelope size guard. `Cauli(max_envelope_bytes=...)`
  covers `.delay()`, `.apply_async()` and the batch calls, but a periodic
  entry with an oversize `args` payload still publishes and still dead letters
  at the worker.
- A dead lettered envelope larger than 4096 bytes yields no recoverable task
  id, so no failure result key is written and a caller waiting on `get()`
  waits out its own timeout. The client side size guard stops a current
  producer creating that state. An older client, or a client whose
  `max_envelope_bytes` is above the worker's `--max-envelope-bytes`, still
  can.
- Both io lanes deep clone the whole `args` and `kwargs` tree per task,
  because the envelope owns them by value. Throughput cost only, not a
  correctness problem. The fix is a coordinated change across four worker
  files and was held back rather than half landed.

### Known limitations

- **Redis Cluster is not supported, and the worker now refuses to start on
  one.** The worker links no cluster protocol at all, so it never follows a
  MOVED redirect and ordinary operations fail against a real multi node
  cluster, not only the delayed and periodic paths. Those fail for a second
  reason as well: the mover's two keys and beat's seed keys never share a hash
  slot, so Cluster rejects every call with CROSSSLOT. Before it touches a
  consumer group the worker sends `INFO cluster` and exits 1 when the reply
  says `cluster_enabled:1`, naming the topology and the loss it would cause.
  Set `CAULI_ALLOW_REDIS_CLUSTER=1` to start anyway and accept that loss. A
  Redis that will not answer the probe at all starts normally, so an ACL that
  blocks `INFO` costs nothing. Standalone is the supported topology. Sentinel
  is reachable from the Python client and `cauli-beat` through
  `Cauli(redis_client=...)`, but the worker connects by URL and never looks a
  master up again after a failover. Hash tags would fix the CROSSSLOT half but
  would change the key naming scheme, and beat's claim atomicity cannot be
  fixed without a single global hash tag, which would pin every key to one
  slot and remove the only reason to run Cluster.
- **Redis must be persistent and must never come back empty.** The stream,
  its consumer group, the pending entries list and the delayed sorted set are
  the only copy of accepted work. A Redis that restarts empty loses all four.
  That is the ElastiCache default, and it is also what an out of memory kill
  or a restore from backup looks like. The worker itself recovers without
  help: `loops.rs` catches `NOGROUP` on the next fetch, recreates the consumer
  groups, logs what that means in plain words, and resumes. Recovering the
  groups is not recovering the work, and nothing in cauli can recover the
  work. Run with AOF.
- **A wedged event loop costs a process restart, taken automatically.** One
  blocking call inside an `async def` starves that loop thread of every
  callback for the life of the process, and CPython offers no safe way to kill
  a thread, so the loop cannot be replaced in place. A watchdog stamps every
  embedded loop every 5 seconds and, when a loop has made no progress for 15
  seconds and a second signal agrees, exits the process with code 87 so the
  supervisor restarts it in about a second. In flight tasks are redelivered
  under the at least once guarantee. Two things this does not do: it does not
  save the wedged task, and it does not stop a blocking call being a bug in
  your task. `loop_lag_ms` in the stats line is the leading indicator;
  `async_rejected` is the lagging one and only moves once 4096 submissions
  have piled up behind the wedge.
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
- **An async task that raises `TimeoutError` itself is reported as the
  worker's own limit.** From Python 3.11 the builtin `TimeoutError` and
  `asyncio.TimeoutError` are the same class, so the `asyncio.wait_for` that
  enforces the async lane's limit catches both and reports
  `TimeLimitExceeded` with origin `"worker"` either way. The sync io and cpu
  lanes tell the two apart exactly. This is inherent to the language, and it
  is the one case the `TimeLimitExceeded` rename above could not separate.
- **The enqueuing client's clock is the one that can still hurt you.** Both
  the worker and beat are anchored on Redis now: `clock.rs` samples Redis
  `TIME`, re anchors on it, and warns at boot about a skew worth naming, so an
  NTP step on a worker no longer writes a far future score into the delayed
  set and every worker in a fleet agrees on when delayed work is due. The
  Python client is not anchored: it stamps `enqueued_at` and `expires_at` from
  `time.time_ns()` on the enqueuing host. A client whose clock runs behind
  Redis shortens every deadline it stamps by the skew, and far enough behind,
  with an app wide `queue_ttl`, the worker discards everything that host
  enqueues. Keep the hosts that enqueue on NTP, not only the workers.
- **The stats line is the whole operator interface, and it is a log line.**
  It now carries per lane latency, `sync_p50`, `sync_p99`, `async_p50`,
  `async_p99`, `cpu_p50` and `cpu_p99`, so a degradation that preserves
  throughput while inflating latency is visible rather than silent, plus
  `pid=` and `host=` so one supervised process can be told from another. What
  is still missing is the shape of it: there is no metrics endpoint, no JSON
  logging, no health endpoint and no queue depth field, all rejected for 1.0
  in `docs/decisions/observability.md`. Scraping means parsing a log line.
  Percentiles are per interval and are drained on read, so a scraper and a
  human tailing the log will not see the same numbers.
- **cpu child memory is invisible in `rss_mb`.** That field covers the worker
  process only; forked children are separate processes and are never summed
  into it, so a child can grow to hundreds of megabytes while `rss_mb` never
  moves. `--cpu-max-tasks-per-child` is the only bound on a child's memory:
  cauli sets no rlimit and no cgroup on it. It defaults to 1000 rather than to
  never, so an ordinary deployment is bounded without doing anything; pass 0
  to opt out. A child killed by the OS OOM killer surfaces as a generic
  `WorkerLost`, the same as any other child death.
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
- **`.delay()` is synchronous Redis I/O, and the async twin is opt in.**
  Inside an async handler `.delay()` blocks the event loop for up to the
  client's 5 second socket timeout when Redis degrades. `AsyncCauli` with
  `await task.adelay(...)` and `await result.aget(...)` is the way out, but it
  is a different app class: an app built as `Cauli` has no awaitable enqueue
  and raises a message saying so. One `AsyncCauli` per event loop, because
  `redis.asyncio` binds its pool to the loop that first uses it.
- **No Python `atexit` handler ever runs.** The worker leaves through an
  immediate process exit, deliberately, because running C library atexit
  handlers while Python threads are live is what caused the corruption
  incident that shaped this design: `OPENSSL_cleanup`, pulled in by a Postgres
  driver's libssl, raced per thread teardown at shutdown and reported a
  corrupted heap as a clean drain. `bench/RESULTS.md` records that discovery,
  the switch to `libc::_exit()` and the 23 clean runs at configurations that
  previously corrupted reliably. The practical cost is that exit time flushes
  do not happen, so Sentry and OpenTelemetry users must flush explicitly, and
  buffered task output at shutdown is lost.
- **A stalled fetch loop delays the start of the drain.** The worker waits
  for the fetch loop to return before it computes the drain deadline. The
  Redis response timeout shrinks that window sharply, since the loop can no
  longer hang forever, but does not close it. Separately, a Redis outage
  during shutdown costs the full `--drain-timeout` every time, because there
  is nothing left to acknowledge against and the deadline is the only thing
  that ends the wait. Size a container's termination grace period above
  `--drain-timeout`, not at it.
- **conda environments cannot run the worker.** The wheel installs and the
  worker then fails before it starts, because the loader cannot find that
  environment's libpython. `pyenv` needs `PYTHON_CONFIGURE_OPTS="--enable-shared"`.
  System CPython, Docker `python` images, venv and virtualenv on top of
  those, distro packages and `actions/setup-python` all work. The worker is
  Linux only, including from source, because it arms `PR_SET_PDEATHSIG`
  unconditionally.
- **Memory over a long run is unverified, and this release does not claim
  otherwise.** The only soak in the tracked benchmark suite was killed by an
  environment outage partway through and never produced a verdict.
  `bench/RESULTS.md` says so under "Soak test", and names what a redo needs.
  Shorter runs have looked flat, but their output is not in this repository,
  so it is not a number this project can defend and it is not published here.
  Nothing has soaked against a workload that deliberately fails, so retries,
  dead letters and expiry are the least exercised paths of all. Watch `rss_mb`
  in the stats line, and remember it does not include cpu children. A soak
  against a failing workload is the routine that continues after this
  release.

### Upgrade notes

**Upgrade workers before producers.** A worker that does not recognise a task
name consumes the message and dead letters it terminally. That was already
true before 1.0; what changed is that it now also writes an
`UnregisteredTask` failure result, so the caller stops waiting and receives a
definitive error rather than blocking. Either way the task does not survive
the rollout to be picked up by a newer worker, so in a rolling deploy every
worker must be running 1.0 and must know the new task name before anything
enqueues it.

**Idempotency keys change shape, so a rolling upgrade can run a guarded task
twice.** The Redis key is now derived with SHA-256 instead of 64 bit FNV-1a,
so a key minted by a pre 1.0 worker never matches the key a 1.0 worker
computes for the same string. A claim written by an old worker is invisible
to a new one. For the length of the rollout, and for as long as any old claim
is still live afterwards, a task carrying an `idempotency_key` can execute
twice. Nothing in cauli can bridge the two derivations. If your workload
cannot tolerate that, drain the queues and stop every old worker before
starting a 1.0 one, rather than rolling. If it can, roll normally and accept
duplicates for the window. Old claim keys are not read again and expire on
their own TTL.

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
- Check every `crontab()` call for three or more positional arguments. Those
  now raise `TypeError`. Pass `day_of_month=`, `month=` and `day_of_week=` by
  name.
- If you match on the dead letter `reason` field, handle `not_retryable`
  alongside `max_retries`.
- If you parse the stats line, it gained `async_rejected` and `cpu_backlog`.
- If your Redis tail latency exceeds 5 seconds under load, raise
  `--redis-timeout`. A false trip costs one log line and at most one
  visibility timeout of added latency on that task, never data loss.
- If the dead letter queue is your audit trail, arrange to drain or export it:
  it now holds roughly the most recent 1000 entries per queue, and the stream
  expires 7 days after the last dead letter written to it.
- If you run with `CAULI_ALLOW_REDIS_CLUSTER=1`, nothing changes: the ack and
  delete transaction touches one key, so it stays inside a single hash slot.
