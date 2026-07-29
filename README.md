# cauli

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
pip install cauli               # client: Python >= 3.10, needs redis>=5
pip install 'cauli[speed]'      # optional: msgspec-accelerated JSON codec (wire format unchanged)
```

The worker is a separate Rust binary, built from source (no crates.io/prebuilt release yet):

```bash
cd worker
cargo build --release --bin cauli-worker
# binary at target/release/cauli-worker
```

## Architecture

```
                    ┌────────────────────────────────────────────────┐
  Django/FastAPI    │  cauli-worker (one Rust process, tokio)         │
  ────────────►     │                                                │
  app.task.delay()  │  fetch / ack / retry / DLQ / delayed mover /   │
        │           │  reclaim-on-crash / idempotency / results      │
        ▼           │                  │                             │
   Redis Streams ◄──┼──────────────────┘                             │
   consumer groups  │        │                    │                  │
                    │        ▼                    ▼                  │
                    │  embedded CPython     child process pool       │
                    │  - asyncio loop(s)    python -m cauli._exec     │
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
# one process, 500 concurrent I/O slots, 6 CPU executors
cauli-worker --app myproj.tasks:app --io-concurrency 500 --cpu-workers 6
```

Requires Redis >= 7.0 as the broker and result backend.

Cpu tasks run in a forked child-process pool by default (one warmed, `gc.freeze()`d parent;
children fork copy-on-write, so respawning after a crash or hard timeout is cheap). Add
`--cpu-child-threads M` to pipeline up to M requests per child on workloads that release the
GIL (e.g. blocking network calls inside a `kind="cpu"` task); `--no-fork-server` falls back to
one process per cpu task (also entered automatically if fork-server startup fails).
`--cpu-prefetch` (default 4) controls how many requests are staged in each child ahead of
what it is executing; raise it for small tasks, lower it for long ones.

## Measured

Drain-rate benchmarks, 6 workers on 6 shared cores, **both stacks tuned at their own optimum**
(full method, caveats and raw numbers in `bench/RESULTS_CPU.md`):

| Workload | Celery best | cauli best | ratio |
|---|---|---|---|
| CPU, 0.5 ms tasks | 672.9/s | 6057.9/s | 9.0x |
| CPU, 2 ms tasks | 630.1/s | 1733.0/s | 2.75x |
| CPU, 51 ms tasks | 67.8/s | 72.6/s | ~parity |
| IO, async | 361.1/s | 18177.9/s | 50x, at 5x less RAM |

**The 51 ms row is the honest one to read first.** Once a CPU task is big enough, both stacks
are simply core-bound and parity is the ceiling: 6 cores of hashing is 6 cores of hashing
whoever schedules it. cauli's advantage is per-task overhead, so it grows as tasks get smaller
and vanishes as they get larger. Benchmarks are single-run on a contended box; treat anything
under ~5% as noise.

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
  untrusted party can write to.

## Repository layout

- `PROTOCOL.md` — the wire/behavior contract (envelope JSON, Redis keys, worker semantics)
- `worker/` — Rust worker binary (`cauli-worker`)
- `py/` — Python package `cauli` (client API + cpu child executor)
- `bench/` — Celery vs cauli benchmark harness (WSL, cgroup capped)

## Status

v0.1: Redis broker only, no chains/chords, no cron/beat, no priorities. See PROTOCOL.md §9.
Worker targets Linux; the client library is cross platform.

## License

Licensed under either of Apache-2.0 or MIT at your option. See `LICENSE-APACHE` and
`LICENSE-MIT`.
