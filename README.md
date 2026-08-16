<p align="center">
  <img src="assets/cauli-logo.png" alt="Cauli" width="200">
</p>

<h1 align="center">cauli</h1>

<p align="center">
  A background task queue for Python.<br>
  Tasks in Python. Worker in Rust.
</p>

<p align="center">
  <a href="#license"><img src="https://img.shields.io/badge/license-MIT%20OR%20Apache--2.0-blue" alt="License"></a>
  <img src="https://img.shields.io/badge/python-3.10%2B-blue" alt="Python 3.10+">
  <img src="https://img.shields.io/badge/broker-Redis%20%E2%89%A5%207.0-red" alt="Redis 7+">
  <img src="https://img.shields.io/badge/worker-Linux-lightgrey" alt="Linux worker">
</p>

---

You write tasks in Python. They run under one Rust binary that embeds CPython,
so thousands of concurrent I/O tasks share a single OS process instead of one
forked process per slot, while CPU tasks still get real multicore parallelism.

Works with Django, FastAPI, Flask or a plain script. Redis is the broker and
the result store.

## Install

```bash
pip install cauli          # client library
pip install cauli-worker   # the worker binary
```

Install the worker into the same virtualenv as your app. `cauli-worker` embeds
CPython, so its wheel is built per CPython minor version and links that venv's
own interpreter. Requires Linux, glibc 2.28 or newer, and a CPython built with
`--enable-shared` (python.org builds, the `python:*` Docker images, Debian,
Ubuntu, Fedora, conda and actions/setup-python all qualify).

Building from source has no such constraint:

```bash
git clone https://github.com/bilalwork31-cyber/Cauli.git
cd Cauli/worker && cargo build --release --bin cauli-worker
```

## Quickstart

```python
# myproj/tasks.py
from cauli import Cauli

app = Cauli(redis_url="redis://localhost:6379/0")

@app.task(max_retries=5)
def send_email(to: str):
    ...

@app.task()                      # async def just works
async def call_api(url: str):
    ...

@app.task(kind="cpu", timeout=120)
def crunch(data: list[int]):
    ...
```

```python
# anywhere in your app
from myproj.tasks import send_email

r = send_email.delay("a@b.com")
r.get(timeout=10)
```

```bash
cauli-worker -A myproj.tasks:app -c 50
```

Tasks stay directly callable in tests: `crunch([1, 2])` runs inline, no broker
needed.

## Concurrency: one flag

`-c` is the number of tasks in flight, the same unit Sidekiq and Hangfire use.
It is **not** a process count: Celery's `-c 50` forks 50 processes, this runs
50 tasks on a fraction of that. Everything else is derived from it, and every
derived value has its own flag if you want to pin it.

```bash
cauli-worker -A myproj.tasks:app -c 500 --print-plan
```

```
cauli-worker execution plan
  cores detected     6
  -c (total)         500
  worker processes   6
  per process:
    io tasks in flight  84  (async + sync together)
    sync io threads     84
    asyncio loops       1
    cpu children        1  (started on first cpu task; only if the app registers kind="cpu" tasks)
  totals: 504 io tasks in flight, 504 sync threads, up to 6 cpu children
```

Coming from Celery:

| Celery | cauli |
|---|---|
| `celery -A myproj worker -P prefork -c 50` | `cauli-worker -A myproj.tasks:app -c 50` |
| `celery -A myproj worker -P threads -c 200` | same command, `-c 200` |
| `celery -A myproj worker -P gevent -c 1000` | same command, `-c 1000` |
| `celery -A myproj worker -Q high,low` | `cauli-worker -A myproj.tasks:app -Q high,low` |
| `celery -A myproj beat` | `cauli-beat --app myproj.tasks:app` |

**There is no `-P` pool flag.** Celery makes you pick one pool per worker, so a
mixed workload becomes three deployments. cauli routes each task to the right
executor by itself, all three at once in the same worker.

## How a task is routed

| You write | Runs on |
|---|---|
| `async def` | embedded asyncio loops |
| `def` | Rust owned thread pool |
| `def` with `kind="cpu"` | forked child processes |

The only judgement call is `kind="cpu"`, and what decides it is whether the
body holds the GIL. `hashlib`, `zlib`, numpy and Pillow release it and run fine
on threads; pure Python loops hold it and need the child processes. Pick
`kind="cpu"` too when a task must be forcibly killable on timeout, or when it
leaks memory and should be recycled.

The cpu pool is one warmed parent that calls `gc.freeze()` and forks children
copy on write, started on the first cpu task rather than at boot.

Full flag reference and tuning guide: [docs/CONFIGURATION.md](docs/CONFIGURATION.md).

## Scheduling

```python
from cauli import crontab, interval

app.add_periodic_task("nightly", nightly_report,
                      crontab(minute=0, hour=3, timezone="Europe/Berlin"),
                      expires=1800)
app.add_periodic_task("heartbeat", "myproj.tasks.ping", interval(30))
```

```bash
cauli-beat --app myproj.tasks:app
```

**Run two replicas.** Schedule state lives in Redis behind a leader lease, and
every firing is an atomic compare and set on the slot, so two instances that
both believe they lead still produce exactly one task per slot. Celery's beat
keeps state in a local file with no locking and its own docs tell you to run
only one.

After downtime a due entry fires once and resumes its cadence; missed slots are
coalesced, never replayed. Per call scheduling is the usual set:

```python
send.apply_async(countdown=30)
send.apply_async(eta=datetime(2030, 1, 1, 9, tzinfo=timezone.utc))
send.apply_async(expires=60)      # discarded unrun if picked up too late
```

`eta` must be timezone aware. A naive datetime raises rather than being guessed
at.

## Django

```bash
pip install 'cauli[django]'
```

```python
# myproj/cauli.py
from cauli.contrib.django import autodiscover_tasks, django_app

app = django_app()
autodiscover_tasks(app)
```

```bash
DJANGO_SETTINGS_MODULE=myproj.settings cauli-worker -A myproj.cauli:app -c 50
```

`django_app()` reads `CAULI_*` settings, calls `django.setup()`, and closes
stale connections around every task with Celery fixup parity. That covers
connection lifecycle, not connection count: every process and every sync
thread keeps its own connection, so totals scale as `procs * io_threads`
plus a smaller async and cpu share, past Postgres's default 100 connections
sooner than the flags suggest. Put a pooler such as pgbouncer, in
transaction mode, in front of Postgres once you approach that; see the
Django settings section of
[docs/CONFIGURATION.md](docs/CONFIGURATION.md) for the full formula.

Enqueueing inside a transaction has an on commit variant, so a task never
observes a row that got rolled back:

```python
with transaction.atomic():
    order = Order.objects.create(...)
    send_receipt.delay_on_commit(order.id)
```

## FastAPI

```bash
pip install cauli 'sqlalchemy[asyncio]' 'psycopg[binary]'   # or asyncpg
```

```python
# myproj/cauli.py
from cauli.contrib.fastapi import fastapi_app

app = fastapi_app("postgresql+psycopg://user:pass@host/db")
```

```bash
cauli-worker -A myproj.cauli:app -c 50
```

`fastapi_app()` builds one `create_async_engine()` and one `async_sessionmaker()`
up front, opens an `AsyncSession` before every task and closes it after through
a `ContextVar` task code reads with `get_session()`, and disposes the engine on
process init so a pooled connection can never survive into a forked cpu child.
Two assumptions it does not enforce in code: the default `--io-loops 1` (an
async pool binds to whichever loop first checks a connection out, so more than
one loop hands the same pool to more than one thread), and no `kind="cpu"`
tasks (`asyncio.get_running_loop()` succeeds inside a cpu task's own body, so
"no loop" is not a safe test there; use a plain synchronous SQLAlchemy engine
on that lane instead). Committing or rolling back stays task code's job,
exactly like `django_app()` leaves connections and transactions separated.

## What you should know before shipping

- **Delivery is at least once.** A worker crash can redeliver a task, so make
  tasks safe to repeat or pass an `idempotency_key`.
- **`--visibility-timeout` must exceed your longest task timeout.** The worker
  warns at startup when a registered task violates it.
- **A sync task's hard timeout cannot kill its thread.** CPython has no safe
  way to stop a running thread: the task is marked failed and a replacement
  thread is spawned, but the original call runs on until it returns. Use
  `kind="cpu"` for work that must be killable.
- **Results and tracebacks are stored in plaintext in Redis.** Keep secrets out
  of task arguments, return values and exception messages.
- **Priorities are deliberately not supported.** Use queue order
  (`-Q high,default,bulk`) or separate worker fleets. Reasoning in
  [PROTOCOL.md](PROTOCOL.md) section 9.4.

## Documentation

| Document | Contents |
|---|---|
| [docs/CONFIGURATION.md](docs/CONFIGURATION.md) | Every flag, setting and environment variable, with a tuning guide |
| [PROTOCOL.md](PROTOCOL.md) | Wire format, Redis keys and worker semantics |
| [worker/ARCHITECTURE.md](worker/ARCHITECTURE.md) | How the worker is built |

## Status

v0.1. Redis only, no chains or chords, no rate limits. The worker targets
Linux; the client library and `cauli-beat` are cross platform. Full list of
limits in [PROTOCOL.md](PROTOCOL.md) section 11.

Performance numbers are deliberately absent from this README. A claim-first,
reproducible benchmark suite lives in [bench/](bench/README.md) — start with
[bench/CLAIMS.md](bench/CLAIMS.md) for what's actually being tested and
[bench/RESULTS.md](bench/RESULTS.md) for the measurements, environment, and
an explicit list of what isn't measured yet. Actively being extended, not a
finished campaign.

## License

Apache-2.0 or MIT, at your option. See `LICENSE-APACHE` and `LICENSE-MIT`.
