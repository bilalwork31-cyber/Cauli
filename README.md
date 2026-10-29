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

cauli is a task queue for Python. You write tasks in Python; one Rust binary,
`cauli-worker`, runs them from Redis.

## Why

- **One worker for every kind of task.** `async def` tasks run on an event
  loop, `def` tasks on a thread pool, `kind="cpu"` tasks in forked child
  processes, all inside one `cauli-worker` command. There is no pool type to
  choose and no second deployment for CPU work.
- **One concurrency flag.** `-c 50` means 50 tasks in flight, not 50
  processes. Processes, threads and slots derive from it; `--print-plan`
  shows the result.
- **At least once delivery.** Redis Streams consumer groups, a visibility
  timeout, retries with backoff, a dead letter queue, idempotency keys, soft
  and hard timeouts. A crashed worker's tasks run again elsewhere.
- **Async native client.** `await task.adelay(...)` never blocks your loop.

## Install

```bash
pip install cauli
```

This installs the client and, on Linux, the prebuilt `cauli-worker` binary.
`cauli-worker --print-plan` checks it without Redis or an app.

## Quickstart

```python
# myproj/tasks.py
from cauli import Cauli

app = Cauli(redis_url="redis://localhost:6379/0")

@app.task(max_retries=5)
def send_email(to: str) -> None:       # sync: runs on the thread pool
    ...

@app.task()
async def call_api(url: str) -> int:    # async: runs on the event loop
    ...

@app.task(kind="cpu", timeout=120)
def crunch(numbers: list[int]) -> int:  # cpu: runs in a child process
    return sum(n * n for n in numbers)
```

```python
from myproj.tasks import crunch

result = crunch.delay([1, 2, 3])
result.get(timeout=10)                  # 14
```

```bash
cauli-worker -A myproj.tasks:app -c 50
```

`-A` takes `module:attr`. A task object stays a plain callable, so
`crunch([1, 2, 3])` runs inline in tests with no broker.

### Django

```python
# myproj/tasks.py
from cauli.contrib.django import django_app

app = django_app("myproj.settings")

@app.task()
def send_receipt(order_id: int) -> None: ...

# in a view, inside transaction.atomic()
send_receipt.delay_on_commit(order.id)  # published only if the transaction commits
```

`django_app` reads `CAULI_*` settings and closes stale connections around
every task, like Celery's Django fixup.

### FastAPI

```python
from fastapi import FastAPI
from cauli import AsyncCauli

cauli = AsyncCauli(redis_url="redis://localhost:6379/0")
api = FastAPI()

@cauli.task()
async def send_email(to: str) -> None: ...

@api.post("/signup")
async def signup(email: str):
    result = await send_email.adelay(email)
    return {"task_id": result.id}
```

For a session per task on async SQLAlchemy, use `cauli.contrib.sqlalchemy`.

## Supported platforms

| Part | Runs on |
|---|---|
| Client and `cauli-beat` | CPython 3.10 to 3.14, any OS |
| Worker | Linux, x86_64 or aarch64, glibc 2.35+, CPython 3.10 to 3.14 built with `--enable-shared`, free threaded 3.14t |
| Broker and result store | Redis 7.0+ standalone, `rediss://` supported. Sentinel from the client and `cauli-beat` only |

No worker wheel exists for musl (Alpine) or free threaded 3.13t; conda is untested. Redis
Cluster, other brokers and canvas primitives (chains, groups, chords) are not
supported. See [PROTOCOL.md section 13](PROTOCOL.md#13-distribution) for the
install marker and the enqueue only install.

## Benchmarks

Method, environment and every losing result: [bench/RESULTS.md](bench/RESULTS.md).
Each number comes from `python -m bench <name>`; [bench/README.md](bench/README.md) says what each one tests.

CPython 3.12, one machine (10 threads), one session, each framework at its best measured
configuration:

| Measurement | cauli | Others |
|---|---|---|
| No-op dispatch, tasks/s | 31,712 async, 19,340 sync | Dramatiq 11,407; taskiq 9,736; Celery (8 nodes) 4,462; arq 1,030 |
| CPU bound, 10 ms / 50 ms tasks, tasks/s | 582 / 119 | Celery prefork 545 / 118 |
| Memory, 10,000 tasks in flight | 152 MiB | Celery gevent 326 MiB; taskiq 120 MiB |
| `kill -9` mid run, tasks lost | 0 | Celery default 8, Dramatiq 24, arq 43 |
| Free threaded 3.14t, CPU bound sync tasks | 4.6x the GIL build | |

## Documentation

| Document | Contents |
|---|---|
| [PROTOCOL.md](PROTOCOL.md) | The specification: keys, envelope, delivery, retries, results, beat, packaging |
| [docs/configuration.md](docs/configuration.md) | Every flag, option and setting with its default, and the sizing formulas |
| [docs/architecture.md](docs/architecture.md) | Worker internals and design decisions |
| [docs/migrating-from-celery.md](docs/migrating-from-celery.md) | What ports, what diverges, in what order to migrate |
| [CONTRIBUTING.md](CONTRIBUTING.md) | Building and testing from source |
| [SECURITY.md](SECURITY.md) | Reporting a vulnerability, and the trust model |
| [CHANGELOG.md](CHANGELOG.md) | Changes per release |

Bugs and questions: [github.com/bilalwork31-cyber/Cauli/issues](https://github.com/bilalwork31-cyber/Cauli/issues).

## Status

cauli 0.1.0, the first public release. The envelope, the Redis key layout and
the stats line key set are stable; any change to them before 1.0 is called out
as breaking in the changelog. There is no third party production mileage yet.

## License

Dual licensed under MIT or Apache 2.0, at your option.
