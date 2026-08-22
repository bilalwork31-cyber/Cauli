# cauli 1.0.0

cauli runs Python background tasks from one Rust binary that embeds CPython.
It is built for FastAPI and Django teams who want a Celery alternative with an
async native client, one worker process to operate instead of a pool per
workload, and a slot cost measured in threads rather than in forked processes.
Redis Streams is the broker and the result store: at least once delivery,
retries, a dead letter queue, idempotency keys and timeouts, with `async def`,
blocking `def` and `kind="cpu"` tasks all running side by side in the same
worker.

This is the first public release. 1.0 means the wire format in
[PROTOCOL.md](https://github.com/bilalwork31-cyber/Cauli/blob/v1.0.0/PROTOCOL.md) and the stats line key set are frozen for the 1.x
series. It does not mean a decade of production mileage. Read
[What is proven, and what is not](#what-is-proven-and-what-is-not) before you
adopt it.

## Highlights

- **One flag for concurrency.** `-c 50` is 50 tasks in flight, not 50
  processes. Process, thread and slot counts derive from it, and
  `cauli-worker --print-plan` shows the derivation with no Redis and no app.
- **Three lanes, one worker.** `async def`, `def` and `kind="cpu"` run in the
  same process. There is no pool flag to pick.
- **Async native enqueue.** `AsyncCauli` gives `await task.adelay(...)`,
  `aapply_async`, `await result.aget(...)` and `astatus` on `redis.asyncio`,
  so a slow Redis cannot stall your event loop. The envelope is byte for byte
  identical to the blocking path's.
- **At least once delivery, always on.** Redis Streams consumer groups plus a
  visibility timeout. Every internal failure resolves toward running the task
  again rather than dropping it, so task bodies must tolerate repeats.
- **TLS works.** `rediss://` connects from the worker and from the Python
  client, so managed Redis with encryption in transit is supported. The worker
  links rustls and trusts the platform certificate store: no build flag, no
  OpenSSL headers. Plain `redis://` stays plaintext.
- **Framework integrations.** `cauli.contrib.django` gives Celery fixup parity
  and `delay_on_commit`. `cauli.contrib.sqlalchemy` gives one async session per
  task for FastAPI, Starlette and Litestar.
- **Safe beat.** Schedule state lives in Redis behind a leader lease, so two
  `cauli-beat` replicas produce one task per slot.
- **Idempotency keys are derived with SHA-256.** If you ran a pre 1.0 build
  from source, read
  [Upgrade and compatibility notes](#upgrade-and-compatibility-notes) before
  you roll.

## Install

```bash
pip install cauli
```

That is the whole install. `cauli` pulls the prebuilt `cauli-worker` binary and
pip puts it on PATH inside your virtualenv. No Rust toolchain, no cargo, no
compiler, no second step. Check it with `cauli-worker --print-plan`, which
needs no Redis and no app.

### Supported matrix

| Requirement | Supported |
|---|---|
| Operating system | Linux only, for the worker |
| Architecture | x86_64, aarch64 |
| CPython | 3.10, 3.11, 3.12, 3.13, 3.14 |
| glibc | **2.35 or newer** |
| Build of CPython | configured with `--enable-shared` |
| Redis | 7.0 or newer, standalone |
| Client (enqueue and `cauli-beat`) | Linux, macOS and Windows |

**The glibc 2.35 floor is deliberate, and it is worth understanding before you
plan a deployment.** The worker wheels are tagged
`manylinux_2_35_{x86_64,aarch64}`, so pip installs them on Ubuntu 22.04,
Debian 12, RHEL 9 and newer, and on every current `python:3.x-slim` image, and
refuses them anywhere older. The floor is set by the runner each wheel is built
on rather than by a manylinux container, because the two requirements are in
direct conflict: the worker embeds CPython and must link libpython dynamically,
and the manylinux images ship a static only CPython, so pyo3 refuses to build
against them at all. A 2.28 floor that cannot build is worth less than a 2.35
floor that ships.

The client installs and enqueues everywhere, macOS and Windows included. Only
the worker is Linux only, including from source, because it arms
`PR_SET_PDEATHSIG` unconditionally. A web process that never runs tasks can
skip the binary:

```bash
pip install --no-deps cauli 'redis>=5' 'msgspec>=0.18'
```

Not supported, and each fails for a stated reason: musl (Alpine) and the free
threaded build have no wheel and fail the install, conda installs the wheel and
then cannot start the worker because the loader cannot find that environment's
libpython, and `pyenv` needs
`PYTHON_CONFIGURE_OPTS="--enable-shared"`. python.org builds, the Docker
`python:*` images, distro packages, venv and virtualenv on top of those, and
`actions/setup-python` all work.

Raw binaries are attached to this release as
`cauli-worker-1.0.0-cp3XX-cp3XX-<platform>.tar.gz` for deployments that are not
a virtualenv. The CPython version stays in the filename because for this binary
it is not optional information. Full packaging rules are in
[PROTOCOL.md](https://github.com/bilalwork31-cyber/Cauli/blob/v1.0.0/PROTOCOL.md) section 13.

## Quickstart

Define the tasks:

```python
# myproj/tasks.py
from cauli import Cauli

app = Cauli(redis_url="redis://localhost:6379/0")

@app.task(max_retries=5)
def send_email(to: str):
    ...

@app.task()                       # async def just works
async def call_api(url: str):
    ...

@app.task(kind="cpu", timeout=120)
def crunch(data: list[int]):
    ...
```

Enqueue from anywhere:

```python
from myproj.tasks import send_email

result = send_email.delay("a@b.com")
result.get(timeout=10)
```

Run the worker:

```bash
cauli-worker -A myproj.tasks:app -c 50
```

`-A` takes `module:attr`, not a module. `-c 50` is 50 tasks in flight. Add
`--print-plan` to see the processes, threads and slots it derives before it
starts.

Inside FastAPI, build the app as `AsyncCauli` and await the enqueue:

```python
# myproj/tasks.py
from cauli import AsyncCauli

cauli = AsyncCauli(redis_url="redis://localhost:6379/0")

@cauli.task(max_retries=5)
async def send_email(to: str) -> None:
    ...
```

```python
# myproj/api.py
from myproj.tasks import send_email

@api.post("/signup")
async def signup(email: str):
    result = await send_email.adelay(email)
    return {"task_id": result.id}
```

`AsyncCauli` is a `Cauli`, so `.delay()` and `.get()` keep working on the same
app. Use one `AsyncCauli` per event loop, because `redis.asyncio` binds its
pool to the loop that first uses it, and `await cauli.aclose()` on shutdown.

## Benchmarks

Two numbers, both from [bench/RESULTS.md](https://github.com/bilalwork31-cyber/Cauli/blob/v1.0.0/bench/RESULTS.md), which is the
only place any figure in this project comes from.

**Environment for both: WSL2 (Ubuntu 24.04), 6 shared vCPUs, 11 GiB RAM, Redis
7.0.15 on a dedicated instance.** A shared virtualized box, not bare metal with
isolated cores. Redis, every worker under test and the harness driving them
compete for the same 6 cores, so read every figure as directional. Throughput
is a drain rate: preload the tasks, start the worker, take the slope of
completions between the 10th and 90th percentile of the run.

- **Dispatch: 30,438 tasks per second**, cauli's async lane at
  `--procs 8 --io-concurrency 96`, on a task body of one `redis.incr` and
  nothing else. On the same box a hand rolled asyncio and redis loop with no
  framework at all reaches 79,792 tasks per second, so cauli captures about 38
  percent of that ceiling. The remaining 62 percent is real framework cost:
  envelope building, JSON encode and decode, retry and idempotency
  bookkeeping, consumer group ack.
- **Memory: 215.7 MiB for 10,000 I/O tasks held in flight**, PSS summed across
  every worker process, using `-c N` and cauli's own derived plan. PSS rather
  than RSS, because RSS counts Celery prefork's copy on write pages once per
  child and would have biased the comparison in cauli's favour.

### Where cauli loses

A table that only shows wins is rigged. Two entries carried over from
[bench/RESULTS.md](https://github.com/bilalwork31-cyber/Cauli/blob/v1.0.0/bench/RESULTS.md) unchanged:

- **SQLAlchemy async ORM: taskiq wins.** On the same insert, taskiq measured
  733.6 tasks per second against cauli's 378.6. Root caused rather than left
  unexplained: the gap is not ORM overhead, it is SQLAlchemy's greenlet based
  async engine, which did not scale with added concurrency in this
  environment. External to cauli, and still a loss.
- **Memory below the crossover.** Celery with `-P gevent` or `-P threads` is
  one process with N greenlets or threads, and it is cheaper than cauli up to
  roughly 4,500 to 6,000 tasks in flight. cauli's floor is higher because it
  always runs a supervisor plus one or more worker processes, each embedding
  its own CPython. It only wins past the crossover, on marginal cost per held
  task.

The full picture, including the CPU bound lane where all five frameworks tie at
50 ms per task, the reliability cliff above 104 slots per process on
`--io-concurrency`, and the Django lane that needs pgbouncer at any real
concurrency, is in [bench/RESULTS.md](https://github.com/bilalwork31-cyber/Cauli/blob/v1.0.0/bench/RESULTS.md). Read
[bench/CLAIMS.md](https://github.com/bilalwork31-cyber/Cauli/blob/v1.0.0/bench/CLAIMS.md) first: it states what each measurement is
allowed to prove.

## Known limitations

Stated before you adopt it, not after. The full list is "Known limitations" in
[CHANGELOG.md](https://github.com/bilalwork31-cyber/Cauli/blob/v1.0.0/CHANGELOG.md), and "Still open" just above it.

- **Redis Cluster is not supported and the worker refuses to start on one.** It
  sends `INFO cluster` before touching a consumer group and exits 1 on
  `cluster_enabled:1`. `CAULI_ALLOW_REDIS_CLUSTER=1` starts anyway and accepts
  the loss. Sentinel is reachable from the Python client and `cauli-beat`
  through `Cauli(redis_client=...)`, but the worker connects by URL and never
  looks a master up again after a failover.
- **Redis must be persistent and must never come back empty.** The stream, its
  consumer group, the pending entries list and the delayed sorted set are the
  only copy of accepted work. Run with AOF. The worker recreates missing
  consumer groups on its own, which recovers the queue, not the work.
- **Delivery is at least once with no way to make it at most once.** Worst case
  executions of one task are `(max_retries + 1) x (redelivery_limit + 1)`,
  which is 20 on the defaults.
- **A wedged event loop costs a process restart, taken automatically.** One
  blocking call inside an `async def` starves that loop for the life of the
  process. A watchdog exits with code 87 after 15 seconds of no progress and
  the supervisor restarts in about a second. In flight tasks are redelivered.
- **A hard timed out sync thread and its database connection are lost until
  restart.** CPython has no safe way to stop a running thread. `sync_abandoned`
  counts each one. Use `kind="cpu"` for work that must be killable.
- **Beat can refire one slot after a failover with replication lag.** The
  guarantee is exactly once per surviving Redis dataset, and `idempotent=True`
  does not close that window because the claim key is in the same lost write.
- **An `async def` task that raises `TimeoutError` itself is reported as the
  worker's own limit.** From Python 3.11 the builtin and `asyncio.TimeoutError`
  are one class, so `asyncio.wait_for` cannot tell them apart. The sync io and
  cpu lanes report the two exactly.
- **The stats line is the whole operator interface, and it is a log line.** No
  metrics endpoint, no JSON logging, no health endpoint, no queue depth field,
  no dashboard. Scraping means parsing a log line.
- **cpu child memory is invisible in `rss_mb`.** That field covers the worker
  process only. `--cpu-max-tasks-per-child`, default 1000, is the only bound on
  a child's memory.
- **No Python `atexit` handler ever runs.** The worker leaves through an
  immediate process exit, deliberately: running C library atexit handlers while
  Python threads are live is what produced the heap corruption incident this
  design was shaped by. Sentry and OpenTelemetry users must flush explicitly.
- **`.delay()` is synchronous Redis I/O and the async twin is opt in.** Inside
  an async handler it blocks the loop for up to the client's 5 second socket
  timeout when Redis degrades. `AsyncCauli` is the way out, and it is a
  different app class.
- **The enqueuing client's clock is not anchored on Redis.** The worker and
  beat are. A client whose clock runs behind Redis shortens every deadline it
  stamps. Keep the hosts that enqueue on NTP, not only the workers.
- **No chains, groups, chords, rate limits or task priorities**, and none are
  planned for 1.x. There is also no CLI for the dead letter queue, queue depth
  or the delayed set, so Celery's `inspect`, `purge` and `control` have no
  equivalent.
- **Memory over a long run is unverified.** See below.

## What is proven, and what is not

Proven, and reproducible from this repository:

- **163 Rust tests, 345 Python tests and 26 cross component integration tests,
  all passing.** The integration tests run a real worker binary, real cpu
  children and a real Redis, not mocks.
- **CI runs the Python suite on 3.10 through 3.14**, clippy under both the
  default and the `test-hooks` feature sets, `cargo audit`, and a packaging job
  that rebuilds the release artifacts and runs them.
- **The release pipeline gates on running the wheels, not on building them.**
  It builds 10 worker wheels, x86_64 and aarch64 across all five CPython
  versions. Every one of them is asserted with `readelf -d` to link libpython
  dynamically, because a statically linked worker would carry the build
  machine's interpreter and silently fail to import your tasks. The five
  x86_64 wheels are additionally installed into a stock `ubuntu:24.04`
  container with `LD_LIBRARY_PATH` empty and run there, and run the full
  integration suite in a clean virtualenv. Nothing is uploaded to PyPI until
  all of that passes.
- **The benchmark suite is in the repository and reruns.** No figure in any
  document in this project is allowed to exist without a harness in `bench/`
  that reproduces it, and the figures that failed that rule were deleted rather
  than restated.

Not proven, stated plainly:

- **No long soak result exists.** A 48 hour soak was started and killed by a
  host outage partway through, and the CSV did not survive. Memory over a long
  run is unverified and this release does not claim otherwise. Nothing has
  soaked against a workload that deliberately fails, so retries, dead letters
  and expiry are the least exercised paths of all. Watch `rss_mb` in the stats
  line, and remember it does not cover cpu children.
- **No third party production usage.** This is a 1.0 that has never run in
  anyone else's production. Everything above is a test suite, a benchmark suite
  and a release verification, which is not the same thing as mileage.
- **No published latency table**, no duplicate delivery test, no Redis round
  trip time sweep beyond localhost, and no CPU pinned re measurement apart from
  one mixed workload retest.

## Upgrade and compatibility notes

Version 0.1.0 was never published, so there is no released version to upgrade
from. This section is for deployments running the 0.1.0 development series from
source. If you are installing cauli for the first time, skip to the
compatibility promise at the end.

Several 1.0 changes turn a previously silent outcome into a raised exception,
so code that ran under 0.1.0 can fail under 1.0. That is the intended
direction, and it is not a quiet upgrade. The full list is "Breaking changes"
in [CHANGELOG.md](https://github.com/bilalwork31-cyber/Cauli/blob/v1.0.0/CHANGELOG.md).

### Idempotency keys change shape, so a rolling upgrade can run a guarded task twice

**Read this before you roll, if any task carries an `idempotency_key`.**

The Redis key is now derived with SHA-256, taking the first 128 bits of the
digest as 32 hex characters, instead of a 64 bit FNV-1a hash. At 64 bits a
collision was silent task loss: the colliding task took the duplicate branch,
was acked, deleted, handed someone else's result and never ran. FNV-1a is also
invertible, so a caller who controls one key could suppress another tenant's
task on purpose. There is no new dependency: the digest is implemented in
crate, and `sha2` is not in `Cargo.toml`.

A key minted by a pre 1.0 worker never matches the key a 1.0 worker computes
for the same string, so a claim written by an old worker is invisible to a new
one. For the length of the rollout, and for as long as any old claim is still
live afterwards, a task carrying an `idempotency_key` can execute twice.
Nothing in cauli can bridge the two derivations.

Two ways forward, and you have to pick one:

1. **Drain the queues and stop every old worker before starting a 1.0 one.**
   No duplicate window at all.
2. **Roll normally and accept duplicates for the window.** Old claim keys are
   never read again and expire on their own TTL.

### Upgrade workers before producers

A worker that does not recognise a task name consumes the message and dead
letters it terminally. It now also writes an `UnregisteredTask` failure result,
so the caller receives a definitive error rather than blocking forever, but the
task still does not survive the rollout. In a rolling deploy every worker must
be running 1.0 and must know the new task name before anything enqueues it.

### Before deploying, check for

- **Duplicate task names.** A second registration under the same name now
  raises `ValueError` at import, so an app that starts today may refuse to
  start.
- **`result_ttl` or `idemp_ttl` of 0 on `Cauli()`, and a negative
  `max_lateness` on a schedule entry.** All three now raise.
- **`.delay()` and `.apply_async()` call sites.** Arguments are validated
  against the task signature and a mismatch raises `TypeError` at the call
  site instead of surfacing on a `.get()` that fire and forget callers never
  make.
- **`crontab()` calls with three or more positional arguments.** Those now
  raise. cauli's field order is `cron(8)`'s, so its third and fourth
  positional arguments are the reverse of Celery's. Pass `day_of_month=`,
  `month=` and `day_of_week=` by name.
- **Imports of `cauli.contrib.fastapi`.** The module is now
  `cauli.contrib.sqlalchemy`. The old path stays as an alias that reexports the
  same objects, so mixing both in one process is safe.
- **Code matching on the dead letter `reason` field.** Handle the new
  `not_retryable` alongside `max_retries`.
- **Code matching on `"TimeoutError"` from a worker enforced time limit.** That
  is now `TimeLimitExceeded` with `origin="worker"`. A task raising its own
  `TimeoutError` stays `TimeoutError` with `origin="task"`.
- **Anything parsing the stats line.** It gained `async_rejected` and
  `cpu_backlog`.
- **The dead letter queue, if it is your audit trail.** It now holds roughly
  the most recent 1000 entries per queue and the stream expires 7 days after
  the last dead letter written to it. Arrange to drain or export it.
- **Your Redis tail latency.** Every worker round trip is now bounded by
  `--redis-timeout`, default 5 seconds. Raise it if your tail exceeds that
  under load. A false trip costs one log line and at most one visibility
  timeout of added latency, never data loss.

### Compatibility promise for 1.x

The envelope, the Redis key layout and the worker semantics in
[PROTOCOL.md](https://github.com/bilalwork31-cyber/Cauli/blob/v1.0.0/PROTOCOL.md) are frozen for the 1.x series, as is the stats
line key set in section 7. The two packages `cauli` and `cauli-worker` pin each
other exactly and are published together, so a mismatched pair is a pip
resolution error rather than a protocol bug at runtime.

## Migrating from Celery

Most of the surface ports directly: `@app.task()`, `.delay()`,
`.apply_async(countdown=..., queue=...)`, `AsyncResult`, `.get(timeout=...)`
and a beat process for periodic work.
[docs/MIGRATING-FROM-CELERY.md](https://github.com/bilalwork31-cyber/Cauli/blob/v1.0.0/docs/MIGRATING-FROM-CELERY.md) has the full mapping
table, the eight divergences that bite silently, the list of what cauli does
not do, and a migration order that works. The two that change behaviour without
raising are `crontab()` field order and `-c`, which counts tasks rather than
processes.

## Thanks, and how to help

The most useful thing anyone can do with this release is run it somewhere real
and report what broke, especially a long run, a workload that fails on purpose,
or an architecture or distribution the release matrix does not cover. Issues
and pull requests are welcome at
<https://github.com/bilalwork31-cyber/Cauli>. `CONTRIBUTING.md` carries the one
rule this project holds itself to hardest: no figure in any document without a
harness in `bench/` that reproduces it. Security reports go through
`SECURITY.md`.

**Full changelog:** [CHANGELOG.md](https://github.com/bilalwork31-cyber/Cauli/blob/v1.0.0/CHANGELOG.md).

## License

Apache-2.0 or MIT, at your option. See `LICENSE-APACHE` and `LICENSE-MIT`.
