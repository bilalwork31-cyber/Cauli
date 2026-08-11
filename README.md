<p align="center">
  <img src="assets/cauli-logo.png" alt="Cauli" width="200">
</p>

<h1 align="center">cauli</h1>

<p align="center">
  A high throughput, low RAM background worker runtime for Python.<br>
  Tasks in Python. Worker in Rust.
</p>

<p align="center">
  <a href="#license"><img src="https://img.shields.io/badge/license-MIT%20OR%20Apache--2.0-blue" alt="License"></a>
  <img src="https://img.shields.io/badge/python-3.10%2B-blue" alt="Python 3.10+">
  <img src="https://img.shields.io/badge/broker-Redis%20%E2%89%A5%207.0-red" alt="Redis 7+">
  <img src="https://img.shields.io/badge/worker-Linux-lightgrey" alt="Linux worker">
</p>

---

A high throughput, low RAM background worker runtime for the Python ecosystem
(Django, FastAPI, Flask, plain scripts). Tasks are written in Python. The worker is a
single Rust binary that embeds CPython and executes thousands of tasks concurrently
inside one OS process.

## Why

Celery's prefork model pins one concurrency slot to one forked OS process. Each fork
carries the full application (commonly ~150 to 250MB RSS), so 100 concurrent slots can
cost 25GB of RAM, and most of those processes spend their lives blocked on network I/O.

cauli splits concurrency by workload class:

| Workload | Celery prefork | cauli |
|---|---|---|
| I/O bound (http, db, email, s3) | 1 slot = 1 process | 1 slot = 1 async task or pooled thread inside ONE process |
| CPU bound | 1 slot = 1 process | small child process pool sized to cores (more than cores buys nothing) |

Result: hundreds to thousands of in flight I/O tasks in roughly the RAM of two Celery
forks, while CPU tasks still get true multicore parallelism.

## Install

```bash
pip install cauli          # client: Python >= 3.10 (deps: redis>=5, msgspec)
pip install cauli-worker   # the worker binary, prebuilt for your interpreter
```

Install the worker into the same virtualenv as your app. That is not a
convention, it is how the worker finds the right interpreter: `cauli-worker`
embeds CPython, so its binary links `libpython3.X.so` and will not start
against a different version. There is one wheel per CPython minor version and
Linux architecture, pip picks the matching one, and the interpreter it then
embeds is that venv's own. You get `cauli-worker` and `cauli-beat` on your PATH.

Requirements for the prebuilt worker: Linux on x86_64 or aarch64, glibc 2.28 or
newer, and a CPython configured with `--enable-shared`. That covers python.org
builds, the Docker `python:*` images, Debian, Ubuntu and Fedora system packages,
conda, and actions/setup-python. `pyenv` does not use it by default, so either
reinstall with `PYTHON_CONFIGURE_OPTS="--enable-shared" pyenv install 3.12` or
build from source.

Building the worker from source, which has no such constraints:

```bash
git clone https://github.com/bilalwork31-cyber/Cauli.git
cd Cauli/worker
cargo build --release --bin cauli-worker
# binary at target/release/cauli-worker
```

## Architecture

```
                    ┌────────────────────────────────────────────────┐
  Django/FastAPI    │  cauli-worker (one Rust process, tokio)         │
  ────────────►     │                                                │
  app.task.delay()  │  fetch / ack / retry / DLQ / delayed mover /   │
        │           │  reclaim-on-crash / idempotency / expiry /     │
        │           │  results         │                             │
        ▼           │                  │                             │
   Redis Streams ◄──┼──────────────────┘                             │
   consumer groups  │        │                    │                  │
        ▲           │        ▼                    ▼                  │
        │           │  embedded CPython     child process pool       │
        │           │  - asyncio loop(s)    python -m cauli._exec     │
        │           │    for async tasks    (cpu tasks, N = cores,   │
        │           │  - thread pool for     hard-kill on timeout)   │
        │           │    sync I/O tasks                              │
        │           └────────────────────────────────────────────────┘
        │
  cauli-beat x N  ── periodic schedules, state in Redis, leader lease
                     (run two replicas; each slot still fires exactly once)
```

Delivery is at least once (Redis Streams consumer groups: ack after completion, pending
entries reclaimed after a visibility timeout if a worker dies). Retries use exponential
backoff with jitter via a delayed ZSET. Exhausted tasks land in a dead letter stream.
Optional idempotency keys dedupe execution. Results land in Redis with a TTL.

## Quickstart

```python
# myproj/tasks.py
from cauli import Cauli

app = Cauli(redis_url="redis://localhost:6379/0")

@app.task(max_retries=5)
def send_email(to: str):
    ...

@app.task(kind="cpu", timeout=120)
def crunch(data: list[int]):
    ...

@app.task()                     # async def just works
async def call_api(url: str):
    ...
```

```python
# anywhere in Django/FastAPI/Flask
from myproj.tasks import send_email
r = send_email.delay("a@b.com")
r.get(timeout=10)
```

```bash
# 500 tasks in flight, one flag. Worker processes, thread pool, async slots
# and cpu children are all sized from it.
cauli-worker -A myproj.tasks:app -c 500
```

Requires Redis >= 7.0 as the broker and result backend.

## Running it like Celery

`-c` is total concurrency, exactly the knob you already know:

| Celery | cauli |
|---|---|
| `celery -A myproj worker -P prefork -c 50` | `cauli-worker -A myproj.tasks:app -c 50` |
| `celery -A myproj worker -P threads -c 200` | same command, `-c 200` |
| `celery -A myproj worker -P gevent -c 1000` | same command, `-c 1000` |
| `celery -A myproj worker -Q high,low` | `cauli-worker -A myproj.tasks:app -Q high,low` |
| `celery -A myproj beat` | `cauli-beat --app myproj.tasks:app` |

**There is no `-P` pool flag, on purpose.** Celery makes you pick one pool per
worker, so a mixed workload means three separate deployments. cauli routes each
task by its registered kind: `async def` runs on the embedded asyncio loops,
plain `def` on the thread pool, `kind="cpu"` on the child process pool. One
worker command replaces all three, at the same time, and picking wrong stops
being possible.

**One warning for Celery migrants: `-c` counts tasks, not processes.** Celery
prefork's `-c 50` forks 50 OS processes. Here `-c 50` means 50 tasks in
flight, which is what Sidekiq, Hangfire and asynq mean by concurrency too.
Same capacity, a fraction of the RAM. Update autoscaler math accordingly.

**`-c` also turns on multiprocessing, proportionally.** The binary starts one
worker process per ~64 slots of `-c`, up to all cores, and supervises them
itself: spawn, restart on death, signal fan out for graceful drain.
Concurrency is divided across them. So `-c 50` on the box that also runs your
Django and Redis stays one quiet process, while `-c 2000` on a dedicated 32
core box fans out across every core, one GIL each (measured: 1 process to 4
was +74% throughput at lower p99, bench3). `--procs N` overrides the count;
without `-c` the worker stays a single process with its standalone defaults.

Run `cauli-worker -A myproj.tasks:app -c 500 --print-plan` to see the exact
processes, threads, slots and cpu children a command will create, without
starting anything.

Every internal knob (`--io-threads`, `--io-concurrency`, `--cpu-workers`, ...)
still exists as an explicit override and always wins over the derivation. The
full reference with the measurements behind each default is in
`docs/CONFIGURATION.md`.

Cpu tasks run in a forked child-process pool by default (one warmed, `gc.freeze()`d parent;
children fork copy-on-write, so respawning after a crash or hard timeout is cheap). The pool
starts on the first cpu task rather than at boot, so registering a rarely used cpu task costs
nothing until it runs (`--eager-cpu` warms it at boot). `--cpu-max-tasks-per-child N` recycles
a child after N tasks, the backstop for leaky C extensions. Add
`--cpu-child-threads M` to pipeline up to M requests per child on workloads that release the
GIL (e.g. blocking network calls inside a `kind="cpu"` task); `--no-fork-server` falls back to
one process per cpu task (also entered automatically if fork-server startup fails).
`--cpu-prefetch` (default 4) controls how many requests are staged in each child ahead of
what it is executing; raise it for small tasks, lower it for long ones.

**"Burns CPU" is not enough to pick `kind="cpu"`.** What decides it is whether
the body holds the GIL: `hashlib`, `zlib`, numpy and friends release it and
parallelise fine on the thread pool, while pure Python loops need the child
processes. The two behave about 4x apart on the same body. Full guidance:
`docs/CONFIGURATION.md`.

## Scheduling

### Periodic tasks (`cauli-beat`)

```python
from cauli import Cauli, crontab, interval

app = Cauli(redis_url="redis://localhost:6379/0")

@app.task()
def nightly_report(): ...

app.add_periodic_task("nightly", nightly_report,
                      crontab(minute=0, hour=3, timezone="Europe/Berlin"),
                      expires=1800)          # not worth running after 03:30

app.add_periodic_task("heartbeat", "myproj.tasks.ping", interval(30))
```

```bash
cauli-beat --app myproj.tasks:app          # or: python -m cauli.beat --app ...
```

**Run two replicas.** Celery's beat keeps last-run times in a local `shelve` file with no
locking, so a second one double fires every cron and its own docs tell you to run only one.
cauli keeps schedule state in Redis: replicas take a lease so one leads and failover is
automatic, and every firing is an atomic compare-and-set on the schedule slot, so even two
instances that both believe they lead produce exactly one task per slot. Measured with three
replicas and the leader SIGKILLed: zero duplicate firings, failover inside the lease
(`--lock-ttl`, default 30s). See PROTOCOL.md §10.5.

Schedule entries live in Redis (`cauli:beat:schedule`), not only in Python config, so a
Django-admin view over them is an addition rather than a rewrite. Entries declared in code are
synced in at startup; entries created directly in Redis are scheduled too and are never
reaped by the code sync.

After downtime a due entry **fires once and then resumes its cadence** — missed slots are
coalesced, never replayed. If a very late firing is worse than none (a 03:00 report at 09:00),
say so per entry: `on_missed="skip", max_lateness=1800`. Crontabs use POSIX `cron(8)`
semantics, including day-of-month OR day-of-week, in an explicit IANA timezone; DST fall-back
fires a repeated wall time once and spring-forward does not drop it.

### eta, expiry, queue TTL, routing

```python
from datetime import datetime, timedelta, timezone

send.apply_async(eta=datetime(2030, 1, 1, 9, tzinfo=timezone.utc))  # absolute
send.apply_async(countdown=30)                                       # relative
send.apply_async(expires=60)     # discarded unrun if not picked up within 60s
```

`eta` must be timezone aware — a naive datetime raises rather than being silently read as UTC
(Celery's `enable_utc` footgun) or as local time. An expired task is **discarded at dispatch,
never executed**: DLQ reason `"expired"`, result status `"expired"`, and `get()` raises
`TaskFailedError(type="Expired")`.

```python
app = Cauli(
    redis_url=...,
    queue_ttl={"*": 3600, "bulk": 300},          # nothing sits in bulk for over 5 min
    task_routes={"myapp.email.*": "emails",      # re-route without editing task code
                 "*.report_*": {"queue": "reports"}},
)
```

Queue precedence: per-call `queue=` > `task_routes` > the task's own `queue=` > `default_queue`.
The queue TTL is enforced by the worker as well as stamped by the client, so it applies to
envelopes produced before it was configured.

**Priorities are deliberately not supported.** Redis Streams have none, and every emulation is
N sub-queues drained in weighted order — which multiplies the consumer groups, PELs and
recovery paths, breaks the single blocking `XREADGROUP`, and starves low-priority work exactly
when the system is busiest. Use queue order (`--queues high,default,bulk` dispatches earlier
queues first within each batch, and cannot starve the later ones) or separate worker fleets,
which is the only thing that gives real isolation anyway. Reasoning in full: PROTOCOL.md §9.4.

## Lifecycle hooks

Per-task and per-process hooks run on every execution path (sync thread pool, asyncio loops,
cpu children) — see PROTOCOL.md §4.8:

```python
@app.before_task          # runs in the task's own thread/process, before it
def setup(): ...

@app.after_task           # runs after every task, all outcome paths
def teardown(): ...

@app.process_init         # once per cauli-managed process, before any task
def init(): ...
```

A hook that raises is logged and skipped; it never fails the task. This is the extension
point framework integrations build on — cauli core depends on no framework.

## Django

The opt-in integration lives in `cauli.contrib.django` (`pip install 'cauli[django]'`):

```python
# myproj/cauli.py
from cauli.contrib.django import autodiscover_tasks, django_app

app = django_app()          # or django_app("myproj.settings")
autodiscover_tasks(app)     # imports <app>.tasks across INSTALLED_APPS

# myproj/store/tasks.py  (any INSTALLED_APPS package)
from myproj.cauli import app

@app.task()
def refresh_prices(sku_id): ...
```

```bash
DJANGO_SETTINGS_MODULE=myproj.settings cauli-worker --app myproj.cauli:app
```

`django_app()` reads config from Django settings (`CAULI_REDIS_URL`, `CAULI_DEFAULT_QUEUE`,
`CAULI_RESULT_TTL`, `CAULI_IDEMP_TTL`), calls `django.setup()` when needed, and registers DB
connection lifecycle hooks with Celery-fixup parity: `close_old_connections` before and after
every task, so `CONN_MAX_AGE` is honored and a connection gone stale across a database
restart or failover is replaced instead of poisoning the worker thread that cached it (set
`CONN_HEALTH_CHECKS = True` so the very first task after a restart succeeds). Import-time
connections are closed once per process — in the fork-server parent before the first fork —
so forked cpu children can never share one inherited socket.

Enqueueing inside a transaction is the classic footgun (`delay()` publishes immediately; the
worker can run the task before the row it references is committed, or after the transaction
rolled back entirely). Every task therefore has an on-commit variant, mirroring Celery 5.4:

```python
with transaction.atomic():
    order = Order.objects.create(...)
    send_receipt.delay_on_commit(order.id)   # publishes only if this commits
```

`delay_on_commit(...)`/`apply_async_on_commit(...)` return `None` (no task id exists until
commit) and require Django only when actually called.

## Measured

Numbers from `bench2/`, a campaign written to be rerun against us: worker
pinned to 4 cores, Redis, Postgres, the mock API and the driver kept off them,
**every arm tuned at its own optimum**, headline cells repeated and reported as
medians. Full method, fairness controls and all 500+ raw cells:
`bench2/RESULTS.md`.

Best tuned Celery arm against best tuned cauli arm, drain rate:

| Workload | Celery best | cauli best | ratio |
|---|---|---|---|
| CPU that releases the GIL (hashlib), 0.5 ms | 889/s | 5018/s | 5.6x |
| same, 2 ms | 805/s | 1434/s | 1.8x |
| same, 50 ms | 58/s | 60/s | parity |
| Pure Python CPU (holds the GIL), 0.5 ms | 895/s | 5228/s | 5.8x |
| same, 2 ms | 812/s | 1430/s | 1.8x |
| same, 50 ms | 66/s | 60/s | 0.9x, Celery wins |
| HTTP GET, 20 ms endpoint, pooled Session | 722/s at 328 MB | 963/s at 51 MB | 1.3x at 6.4x less RAM |
| Django ORM write | 264/s at 357 MB | 608/s at 57 MB | 2.3x at 6.2x less RAM |

**The 50 ms rows are the honest ones to read first.** Once a task is big
enough, both stacks are core bound and parity is the ceiling: 4 cores of
hashing is 4 cores of hashing whoever schedules it. cauli's advantage is per
task overhead, so it grows as tasks get smaller and vanishes as they get
larger. At 50 ms of pure Python, Celery prefork wins outright, and the table
says so.

The CPU rows are two workloads on purpose: a body that releases the GIL
parallelises on threads and one that holds it does not, and the two behave
about 4x apart under the same label. Any benchmark that just says "CPU" is
hiding one of them.

**Async is where the gap is structural** (`bench3/`, five async capable
workers, a 20 ms task on the same 4 pinned cores): peak drain was cauli
11,300/s, taskiq 7,100/s, SAQ 3,500/s, arq 900/s. The mechanism is in the
data: arq spends 30.8 redis ops per task against cauli's 3.1. Celery does not
place: its gevent pool drained 125/s through Celery's consumer on work that
raw gevent does at 36,000/s.

## Semantics & limits

- **At-least-once delivery.** A task can run more than once (worker crash + redelivery, or a
  connection drop mid-completion-write). Design tasks to be safe to repeat, or use
  `idempotency_key` to dedupe.
- **`--visibility-timeout` must exceed your longest task's `timeout`.** The worker warns at
  startup if a registered task's timeout doesn't satisfy this, and the recovery loop itself
  only reclaims an entry once it has been idle longer than its own task timeout (not just the
  visibility floor) — but a badly undersized visibility_timeout still means genuine crash
  recovery is slower than it needs to be. See PROTOCOL.md §4.4.
- **A sync (thread-pool) task's hard timeout cannot kill the thread.** CPython has no safe way
  to force-stop a running thread; on hard timeout the worker marks the task failed and moves
  on, spawning a replacement thread so pool capacity isn't lost, but the original call keeps
  running in the background until it returns on its own. Prefer `kind="cpu"` for work that
  must be forcibly killable, or cooperative code that checks a deadline for long sync tasks.
- **Task tracebacks and results are stored in plaintext in Redis** (`cauli:result:*`, DLQ
  entries). Anyone with read access to your Redis instance can see them — don't put secrets or
  PII in exception messages or task arguments/return values if that's a concern for your
  deployment.
- **The worker's working directory is on `sys.path`.** `--app` imports are resolved with CWD
  prepended (so relative app modules just work); don't run `cauli-worker` from a directory an
  untrusted party can write to. Same for `cauli-beat`.
- **A scheduled slot fires at most once, and once per slot.** The beat claim is atomic, so a
  leader that dies cannot both consume a slot and fail to publish it. What a crash does cost is
  lateness: the schedule stalls until the lease expires (bounded by `--lock-ttl`), and the
  slots inside that window collapse into a single firing rather than being replayed. Size
  `--lock-ttl` against how late a scheduled task may acceptably run.
- **Beat is not supported on Redis Cluster.** Its atomic claim-and-publish script spans the
  beat keys and the queue key, which are in different hash slots. It detects the CROSSSLOT
  error, warns, and degrades to claim-then-publish: still no duplicates, but a crash in that
  gap drops one firing.

## Repository layout

- `PROTOCOL.md` — the wire/behavior contract (envelope JSON, Redis keys, worker semantics,
  scheduling controls §9, periodic scheduler §10)
- `docs/` — configuration reference and tuning guide (`docs/CONFIGURATION.md`)
- `worker/` — Rust worker binary (`cauli-worker`)
- `py/` — Python package `cauli` (client API, `cauli-beat` scheduler, cpu child executor)
- `bench2/` — cauli vs Celery campaign, method and results (`bench2/RESULTS.md`)
- `bench3/` — five worker campaign: cauli, Celery, arq, SAQ, taskiq
- `bench/` — the original harness (superseded by bench2)

## Status

v0.1: Redis broker only, no chains/chords, no rate limits. Priorities are a documented
non-feature rather than a gap (PROTOCOL.md §9.4). Full list: PROTOCOL.md §11.
Worker targets Linux; the client library and `cauli-beat` are cross platform.

## License

Licensed under either of Apache-2.0 or MIT at your option. See `LICENSE-APACHE` and
`LICENSE-MIT`.
