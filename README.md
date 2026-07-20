# rupy

A high throughput, low RAM background worker runtime for the Python ecosystem
(Django, FastAPI, Flask, plain scripts). Tasks are written in Python. The worker is a
single Rust binary that embeds CPython and executes thousands of tasks concurrently
inside one OS process.

## Why

Celery's prefork model pins one concurrency slot to one forked OS process. Each fork
carries the full application (commonly ~150 to 250MB RSS), so 100 concurrent slots can
cost 25GB of RAM, and most of those processes spend their lives blocked on network I/O.

rupy splits concurrency by workload class:

| Workload | Celery prefork | rupy |
|---|---|---|
| I/O bound (http, db, email, s3) | 1 slot = 1 process | 1 slot = 1 async task or pooled thread inside ONE process |
| CPU bound | 1 slot = 1 process | small child process pool sized to cores (more than cores buys nothing) |

Result: hundreds to thousands of in flight I/O tasks in roughly the RAM of two Celery
forks, while CPU tasks still get true multicore parallelism.

## Architecture

```
                    ┌────────────────────────────────────────────────┐
  Django/FastAPI    │  rupy-worker (one Rust process, tokio)         │
  ────────────►     │                                                │
  app.task.delay()  │  fetch / ack / retry / DLQ / delayed mover /   │
        │           │  reclaim-on-crash / idempotency / results      │
        ▼           │                  │                             │
   Redis Streams ◄──┼──────────────────┘                             │
   consumer groups  │        │                    │                  │
                    │        ▼                    ▼                  │
                    │  embedded CPython     child process pool       │
                    │  - asyncio loop(s)    python -m rupy._exec     │
                    │    for async tasks    (cpu tasks, N = cores,   │
                    │  - thread pool for     hard-kill on timeout)   │
                    │    sync I/O tasks                              │
                    └────────────────────────────────────────────────┘
```

Delivery is at least once (Redis Streams consumer groups: ack after completion, pending
entries reclaimed after a visibility timeout if a worker dies). Retries use exponential
backoff with jitter via a delayed ZSET. Exhausted tasks land in a dead letter stream.
Optional idempotency keys dedupe execution. Results land in Redis with a TTL.

## Quickstart

```python
# myproj/tasks.py
from rupy import Rupy

app = Rupy(redis_url="redis://localhost:6379/0")

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
# one process, 500 concurrent I/O slots, 6 CPU executors
rupy-worker --app myproj.tasks:app --io-concurrency 500 --cpu-workers 6
```

## Repository layout

- `PROTOCOL.md` — the wire/behavior contract (envelope JSON, Redis keys, worker semantics)
- `worker/` — Rust worker binary (`rupy-worker`)
- `py/` — Python package `rupy` (client API + cpu child executor)
- `bench/` — Celery vs rupy benchmark harness (WSL, cgroup capped)

## Status

v0.1: Redis broker only, no chains/chords, no cron/beat, no priorities. See PROTOCOL.md §9.
Worker targets Linux; the client library is cross platform.
