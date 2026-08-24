<p align="center">
  <img src="assets/cauli-logo.png" alt="Cauli" width="200">
</p>

<h1 align="center">cauli</h1>

<p align="center">
  <a href="#license"><img src="https://img.shields.io/badge/license-MIT%20OR%20Apache--2.0-blue" alt="License"></a>
  <img src="https://img.shields.io/badge/python-3.10%2B-blue" alt="Python 3.10+">
  <img src="https://img.shields.io/badge/broker-Redis%20%E2%89%A5%207.0-red" alt="Redis 7.0+">
  <img src="https://img.shields.io/badge/worker-Linux-lightgrey" alt="Linux worker">
</p>

|  |  |
|---|---|
| **Version** | 1.0.0 |
| **Source** | https://github.com/bilalwork31-cyber/Cauli |
| **Download** | https://pypi.org/project/cauli/ |
| **Protocol** | [PROTOCOL.md](PROTOCOL.md) |
| **Keywords** | task, queue, job, async, redis, python, worker, celery, django, fastapi |

## What is cauli?

cauli is a task queue for Python. You write tasks in Python, and one Rust
binary runs them.

A task queue distributes work across threads or machines. Your web process puts
a message on a Redis queue, and a worker process picks it up and runs it. cauli
is the worker, plus the Python client that talks to it.

What makes it different from the alternatives is the worker. Async tasks,
blocking tasks and CPU bound tasks all run inside one process, and one flag
sets the concurrency for all three. There is no pool type to choose.

```python
from cauli import Cauli

app = Cauli(redis_url="redis://localhost:6379/0")

@app.task()
def send_email(to):
    return "sent"
```

## What do I need?

cauli 1.x runs on:

- CPython 3.10, 3.11, 3.12, 3.13, 3.14
- Redis 7.0 or newer, as broker and result store

The **client** installs and enqueues anywhere, macOS and Windows included.

The **worker** is Linux only: x86_64 and aarch64, glibc 2.35 or newer, on an
interpreter built with `--enable-shared` (python.org, the `python:*` Docker
images, distro packages, uv). musl (Alpine), conda and the free threaded build
have no worker wheel yet.

Redis is the only broker. There is no RabbitMQ, SQS or Pub/Sub transport.

## Get Started

```bash
pip install cauli
```

That is the whole install. The prebuilt `cauli-worker` binary comes with it and
pip puts it on PATH. No Rust toolchain, no compiler, no second step. Check it
with `cauli-worker --print-plan`, which needs no Redis and no app.

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

`-A` takes `module:attr`, not a module. `-c 50` means 50 tasks in flight, the
unit Sidekiq and Hangfire use. Add `--print-plan` to see the processes, threads
and slots it derives.

## cauli is...

**Simple.** One flag. `-c 50` is 50 tasks in flight, not 50 processes. Process,
thread and slot counts all derive from it, and `--print-plan` shows the
derivation before anything starts. No configuration file.

**Async native.** `async def` tasks execute directly on an event loop, which
Celery cannot do at all. On the client side `await send_email.adelay(...)`
never blocks your event loop.

**One process.** Async, blocking and CPU bound tasks share a single worker
process. Celery needs a separate worker per pool type, so a mixed workload
means several deployments; cauli routes with `kind="cpu"` on the task instead.

**Correct under crash.** Redis Streams consumer groups plus a visibility
timeout give at least once delivery. A `kill -9` at 160 of 500 tagged tasks
lost 0 of them, and recovery took 34 seconds.

**Cheap to hold work.** A held task costs about 6.6 KiB, so 10,000 tasks in
flight fit in 215.7 MiB. See [Benchmarks](#benchmarks), including where that
loses.

**Stable.** The envelope and the Redis key layout in [PROTOCOL.md](PROTOCOL.md)
are frozen for the 1.x series.

## It supports...

**Execution lanes**

- `async def` tasks on an event loop, uvloop by default
- `def` tasks on a thread pool
- `kind="cpu"` tasks on forked child processes, for real multicore

**Transports and stores**

- Redis 7.0+ as broker, including `rediss://` TLS
- Redis as result store, with a per task `store_result` switch

**Serialization**

- JSON on the wire, frozen in [PROTOCOL.md](PROTOCOL.md)
- msgspec when installed, stdlib `json` otherwise, same bytes either way

**Reliability**

- at least once delivery, retries with exponential backoff and jitter
- dead letter queue, idempotency keys, soft and hard task timeouts
- `cauli-beat` scheduling behind a Redis leader lease, so two replicas produce
  one task per slot

## Framework Integration

| Framework | Integration |
|---|---|
| Django | `cauli.contrib.django` |
| FastAPI | `cauli.contrib.sqlalchemy` |
| Starlette | `cauli.contrib.sqlalchemy` |
| Litestar | `cauli.contrib.sqlalchemy` |
| Flask | not needed |

The integration packages are not strictly necessary, but they add the hooks
that matter: closing database connections around each task, and a session per
task.

### Django

```python
# myproj/tasks.py
from cauli.contrib.django import django_app

app = django_app("myproj.settings")

@app.task()
def send_receipt(order_id: int): ...
```

```python
# in a view, inside transaction.atomic()
order = Order.objects.create(...)
send_receipt.delay_on_commit(order.id)
```

The task publishes when the transaction commits, and not at all if it rolls
back. Arguments are validated at the call site, so a value that cannot be
serialized raises inside your `atomic()` block instead of at COMMIT with the
row already written.

Under `django.test.TestCase` nothing is ever enqueued, because the test itself
runs inside an atomic block that always rolls back. Wrap the assertion in
`self.captureOnCommitCallbacks(execute=True)`, or subclass
`TransactionTestCase`.

### FastAPI

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
from fastapi import FastAPI
from myproj.tasks import send_email

api = FastAPI()

@api.post("/signup")
async def signup(email: str):
    result = await send_email.adelay(email)
    return {"task_id": result.id}
```

`AsyncCauli` is a `Cauli`, so `.delay()` and `.get()` keep working on the same
app and the envelope is identical either way. Use one `AsyncCauli` per event
loop, and `await cauli.aclose()` on shutdown.

## Benchmarks

<!-- assets/benchmark.svg is NOT shown here: it encodes the earlier measurement
     round (30,438 cauli async, 9,622 taskiq, 850.6 celery) and would contradict
     the tables below, which come from a later re-measurement with CPU
     attribution. Regenerate it from the current numbers before restoring it. -->

Full method, every losing result, and the "not yet measured" list live in
[bench/RESULTS.md](bench/RESULTS.md). Read [bench/CLAIMS.md](bench/CLAIMS.md)
first: it states what each measurement is allowed to claim.

**Environment.** WSL2 (Ubuntu 24.04), 6 shared vCPUs, 11 GiB RAM. Redis,
PostgreSQL, every worker under test and the harness driving them compete for
the same 6 cores. This is a shared virtualized box, not bare metal with
isolated cores, so treat every number as directional.

### Dispatch throughput

No database, no ORM, no web framework: the task body is one `redis.incr`, so
this is the queue runtime and nothing else. The `cores` columns are CPU cores
consumed during the measured window, which is what tells you whether a number
is a ceiling or just a saturated box.

| Framework | Best config | Tasks/s | Worker cores | Redis cores | Limited by |
|---|---|---:|---:|---:|---|
| **cauli async** | `--procs 6 --io-concurrency 96` | **27,060** | 4.71 | 0.60 | the box |
| **cauli sync** | `--procs 12 --io-threads 80` | **19,299** | 4.80 | 0.53 | the box |
| taskiq | `--workers 6 --max-async-tasks 100` | 7,812 | 4.54 | 0.40 | itself |
| Celery prefork | `-c 4 --prefetch-multiplier=1` | 826 | 1.13 | 0.17 | itself |

**cauli async is 3.5x taskiq and 32.8x Celery prefork** on the same box in the
same session.

Three things that matter more than the headline number:

**Redis is not the bottleneck for anyone.** It peaks at 0.63 of 6 cores, about
10% of the machine. No row here is broker limited.

**Celery is limited by Celery, at 1.13 cores.** It leaves four of six cores
idle and cannot turn more hardware into more throughput on this workload. cauli
saturates the machine instead. That is the structural difference; the ratio is
just its consequence.

**cauli's actual ceiling is unknown.** Every cauli row is box bound at 4.6 to
4.9 cores while sharing six cores with Redis and the harness. The defensible
claim is "at least 27,060/s using 4.71 cores", not "cauli maxes out at 27,060".
Finding the real number needs a machine where the worker is not competing with
the broker.

These are single runs. The ratios are large enough to survive the noise; the
exact peak config is not. Celery measured 826/s here against 850.6/s from an
independent earlier run, within 3%, which is the cross check that makes the
table credible.

### Django ORM

One `objects.create()` per task, with Postgres deliberately made cheap
(UNLOGGED table, `synchronous_commit=off`) so the row measures the framework
rather than WAL fsync.

| Framework | Best config | Tasks/s | Worker cores | Postgres cores |
|---|---|---:|---:|---:|
| **cauli sync** | `--procs 6 --io-threads 24` | **4,256** | 3.92 | 0.76 |
| Celery prefork | `-c 16` | 247 | 3.24 | 0.16 |

**17.2x, and both sides are framework bound with Postgres under one core.**
Celery spends 3.24 cores to produce 247 tasks/s; cauli spends 3.92 to produce
4,256. Nearly the same CPU, seventeen times the output.

### SQLAlchemy async ORM, the FastAPI shape

| Framework | Config | Tasks/s | Worker cores |
|---|---|---:|---:|
| **cauli async** | `--procs 4 --io-concurrency 32` | **2,032** | 3.88 |
| taskiq | `--workers 4 --max-async-tasks 32` | 1,736 | 3.88 |

Both framework bound at **identical CPU**, cauli 1.17x ahead. A separate
paired run at higher concurrency measured **1.145, 95% CI 1.078 to 1.211,
cauli ahead in 6 of 6 pairs**. Two independent methods within 3% of each other.

*This lane previously published the opposite result: taskiq winning 733.6/s to
378.6/s. That was a harness bug. `sqla_models.make_engine` used
`pool_size=2, max_overflow=98`, and SQLAlchemy closes overflow connections on
return, so nearly every insert paid a fresh connect and handshake. Both
frameworks roughly doubled once it was fixed. The full retraction is in
[bench/RESULTS.md](bench/RESULTS.md).*

### Memory per unit of concurrency

PSS, not RSS, summed across every worker process. *Carried over from the
earlier measurement round, not re-measured with the paired method.*

| Tasks in flight | cauli async | Celery prefork | Celery gevent |
|---:|---:|---:|---:|
| 100 | 58.8 MiB | 2,036.7 MiB | **46.7 MiB** |
| 1,000 | 156.9 MiB | not attempted | **68.2 MiB** |
| 10,000 | **215.7 MiB** | not attempted | 282.9 MiB |

**cauli loses this below roughly 4,500 to 6,000 tasks in flight.** Celery with
`-P gevent` is one process with N greenlets and is cheaper until the crossover.
cauli's floor is higher because it always runs a supervisor plus a worker
process embedding CPython. It wins past the crossover because its marginal cost
is about 6.6 KiB per held task against 19 to 30 KiB.

### CPU bound work

`kind="cpu"` against four alternatives, all at 6 processes. *Carried over, not
re-measured.*

| Task size | cauli | Celery | taskiq | Dramatiq |
|---:|---:|---:|---:|---:|
| 0.5ms | **2,664.8/s** | 778.4/s | 810.5/s | 1,539.5/s |
| 2ms | **1,776.6/s** | 693.2/s | 763.2/s | 1,188.7/s |
| 10ms | 516.5/s | 413.0/s | 515.6/s | 473.3/s |
| 50ms | 115.3/s | 110.8/s | 117.6/s | 117.3/s |

Dispatch overhead is a shrinking fraction of total time as the task grows.
**At 50ms all four are within noise.** At 10ms taskiq ties cauli. Only at small
task sizes does the lead matter.

### Correctness under a hard crash

`kill -9` at 160 of 500 uniquely tagged tasks, restart, count what comes out.
*Carried over. These are counts rather than rates, so box drift cannot move
them.*

| Framework | Lost | Duplicates | Recovery |
|---|---:|---:|---:|
| cauli | **0** | 0 | 34.0s |
| Celery, `acks_late`, `visibility_timeout=5` | 0 | 0 | 103.2s |
| Celery, plain default | 80 of 500 | 0 | never recovered |
| Dramatiq, default | 85 of 500 | 0 | timed out |
| arq, default | 400 of 500 | 0 | timed out |

Dramatiq and arq ran at their default reliability configuration and were not
given the tuned second pass Celery got.

### Where cauli loses

A table that only shows wins is rigged.

| Result | Number |
|---|---|
| **Memory below the crossover** | Celery gevent and threads are cheaper up to roughly 4,500 to 6,000 tasks in flight. |
| **CPU bound parity at 50ms** | All frameworks within noise. taskiq ties at 10ms. |
| **The wrong config stalls under a CPU burst** | A 50ms burst every 3 seconds pushes the naive async lane to 18x baseline p99. Routing it to `kind="cpu"` brings that to 4.0x, ahead of arq at 4.4x and Celery prefork at 14.5x, but the naive number is what you get if you do not route CPU work. |
| **Throughput falls off a cliff** | Above 104 slots per process, a run reaches 91 to 99% and then hangs instead of slowing down. |
| **Django needs pgbouncer at high concurrency** | At `--procs 12 --io-threads 80` the Django lane asks Postgres for 960 connections and exhausts `max_connections`. Django has no connection pool, and the same wall hits Celery with enough prefork workers. |

### What is not measured

Absence is not evidence.

- **No soak result.** A 48 hour soak was killed by a host outage and the data
  did not survive. Memory over a long run is unverified.
- **No published latency table.** The harness works, the table does not exist.
- **No CPU pinned re measurement** apart from one mixed workload retest.
- **cauli's true ceiling.** Every dispatch row is box bound; a machine with the
  broker on separate cores is needed.
- No duplicate delivery test, no payload size sweep, no result round trip
  latency.

### Rerun it

```bash
cd bench
./setup.sh
python3 campaign_paired.py --suite dispatch --pairs 8
```

`campaign_paired.py` interleaves the two lanes and alternates their order every
pair, then reports the mean of per pair ratios with a confidence interval. A
block design is not safe here: on this box one binary and one config drifted
from 19,532 to 12,540 tasks per second across a single session while staying
stable to 4% inside any five minute window, which a block design would have
reported as a difference between frameworks.

`setup.sh` never touches a Redis or Postgres instance you already run.

## Documentation

- [PROTOCOL.md](PROTOCOL.md) is the wire contract and is authoritative
- [docs/CONFIGURATION.md](docs/CONFIGURATION.md) is every flag and the `-c`
  derivation formulas
- [docs/MIGRATING-FROM-CELERY.md](docs/MIGRATING-FROM-CELERY.md) maps Celery
  concepts onto cauli
- [CHANGELOG.md](CHANGELOG.md)

## Limitations

Stated up front rather than discovered later.

- **Redis only.** No RabbitMQ, SQS or Pub/Sub.
- **Linux only worker.** The client runs anywhere.
- **At least once, not exactly once.** A task can run twice after a crash.
  Idempotency keys narrow the window; they do not close it.
- **No workflow primitives.** No chains, groups, chords or canvas.
- **No web UI.** Stats go to stdout as structured logs.

## Getting Help

Report bugs and ask questions at
[github.com/bilalwork31-cyber/Cauli/issues](https://github.com/bilalwork31-cyber/Cauli/issues).
See [CONTRIBUTING.md](CONTRIBUTING.md) to build from source, and
[SECURITY.md](SECURITY.md) to report a vulnerability.

## Status

cauli 1.0.0, the first public release. 1.0 means the wire format in
[PROTOCOL.md](PROTOCOL.md) and the stats line key set are frozen for the 1.x
series. It does not mean a decade of production mileage.

## License

Dual licensed under MIT or Apache 2.0, at your option.
