# Configuration reference

Every knob cauli has: the worker command line, the app object, the task
decorator, per call options, environment variables, and the Django settings
the contrib layer reads.

Where a default was chosen by measurement rather than taste, the measurement
is cited. Figures marked *(bench2)* come from `bench2/RESULTS.md` on a six
core i7-9750H under WSL2 with four cores pinned to the worker; they are the
shape of the effect, not a promise about your machine.

- [Choosing a task kind](#choosing-a-task-kind)
- [Worker command line](#worker-command-line)
- [App object](#app-object)
- [Task decorator](#task-decorator)
- [Per call options](#per-call-options)
- [Environment variables](#environment-variables)
- [Django settings](#django-settings)
- [Tuning guide](#tuning-guide)

## Choosing a task kind

`kind` is the single most consequential setting, and it is the one people get
wrong. It selects which of the worker's three execution models runs the task.
One worker process runs all three at once; you do not run separate workers per
model the way Celery needs separate workers per pool.

| Task | `kind` | Runs on | Celery's equivalent |
|---|---|---|---|
| `def`, waits on network or disk | `"io"` (default) | Rust owned OS thread pool | `-P threads` |
| `async def` | `"io"` (default) | Embedded asyncio loops | `-P gevent` |
| `def`, burns CPU in Python | `"cpu"` | Forked child processes | `-P prefork` |

The distinction that decides it is **whether the body holds the GIL**:

- **Releases the GIL** (`hashlib`, `zlib`, most of numpy, Pillow, any C
  extension that calls `Py_BEGIN_ALLOW_THREADS`, and all blocking socket IO):
  `kind="io"` parallelises it across the thread pool. No process pool needed.
- **Holds the GIL** (pure Python loops, business logic, templating, hand
  written parsing): a thread pool of any width executes these one at a time.
  Use `kind="cpu"`.

Measured on identical bodies *(bench2)*: at 50 ms per task the thread pool
reaches 59 tasks/s on GIL releasing work and 15 tasks/s on GIL holding work,
with the per task time inflating from 62.6 ms to 413.4 ms as concurrency rises
from 1 to 8. That inflation is the GIL serialising the bodies, and it is why
"CPU bound" on its own is not enough information to pick a kind.

Two other reasons to pick `kind="cpu"`:

- **It must be forcibly killable.** CPython cannot safely stop a running
  thread, so a sync io task that overruns its hard timeout is marked failed
  while the call keeps running in the background. A cpu child can be killed.
- **It is memory hungry or leaks.** A child process can be recycled.

## Worker command line

`cauli-worker -A myproj.tasks:app -c 50`

### The two flags that matter

| Flag | Default | Effect |
|---|---|---|
| `-c`, `--concurrency` | unset | Total tasks in flight across all worker processes. **Tasks, not processes**: Celery prefork's `-c 50` forks 50 processes, this `-c 50` runs 50 tasks on far less. Setting it derives everything below |
| `--procs` | 1, or with `-c` one process per ~64 slots up to all cores | Worker processes. The binary supervises them itself: spawn, restart on death, signal fan out for graceful drain |
| `--print-plan` | off | Print the derived plan (processes, threads, slots, cpu children) and exit. Needs no app and no Redis |

With `-c` set, the worker derives its internals from it. Every derived value
can still be pinned by passing its flag explicitly; an explicit flag always
wins. The rules, each from a measured result:

| Derived | Formula | Why |
|---|---|---|
| `--procs` | min(cores, c / 64) | Each process is one GIL: fanning out was +74% throughput at lower p99 (bench3). Scaling by `-c` keeps a small queue worker to one quiet process on a box shared with your web app and Redis, while a large `-c` on a dedicated box uses every core. The 64 slot target is a chosen default, not yet a swept one |
| `--io-concurrency` | c / procs | The gate is the real bound for `async def` tasks; a slot costs about 4 KB |
| `--io-threads` | min(c, 512) / procs, at most the gate | The sync knee is near 1000 threads per process; past it throughput and latency fall together. Staying at 1x the gate is the latency honest default: oversubscribing buys throughput by inflating task p99 (measured 2048 slots on 4 cores: a 20 ms body reached a 92 ms p99) |
| `--cpu-workers` | min(cores, c) / procs | More cpu children than cores buys nothing, and more than `-c` would make `-c 8` on a cpu queue mean something other than 8 |
| `--io-loops` | always 1 | 1 loop beat 2, 3 and 4 in every sweep: extra loops contend for one GIL |

Without `-c` nothing changes: one process, the standalone defaults below.

### Wiring

| Flag | Default | Effect |
|---|---|---|
| `-A`, `--app` | *required* | App location as `module:attr`. The worker's working directory is prepended to `sys.path`, so relative module paths resolve |
| `-Q`, `--queues` | `app.default_queue` | Comma separated queue names to consume |
| `--redis-url` | from app | Precedence: this flag > `CAULI_REDIS_URL` > `app.redis_url` |
| `--python` | `python3` | Interpreter used to spawn cpu children |

### IO execution

| Flag | Default | Effect |
|---|---|---|
| `--io-threads` | 64, or derived from `-c` | OS threads for sync `kind="io"` tasks. **This is the scaling axis for sync work** |
| `--io-concurrency` | 256, or derived from `-c` | Admission semaphore: maximum io tasks in flight, sync and async together |
| `--io-loops` | 1 | Embedded asyncio event loop threads for `async def` tasks |

`--io-concurrency` is a gate, not a worker count. Raising it above
`--io-threads` does not add sync parallelism, it only lets more tasks queue
behind the same threads. Measured *(bench2)*: with `--io-threads 500`, gates of
500, 2000 and 4000 all produced the same 247 to 251 tasks/s, because a two
second task on 500 threads is 250 tasks/s whatever the gate says.

For `async def` tasks the gate *is* the bound, because a slot costs a coroutine
and a socket rather than a thread.

### CPU execution

| Flag | Default | Effect |
|---|---|---|
| `--cpu-workers` | min(cores, c) / procs | Child processes for `kind="cpu"` tasks |
| `--cpu-child-threads` | 1 | Requests pipelined per child, matched by id. Range 1 to 1024 |
| `--cpu-prefetch` | 4 | Requests staged in a child's socket buffer beyond the one it is running. 0 disables |
| `--cpu-max-tasks-per-child` | 0 (never) | Recycle a child after this many completed tasks. The backstop for leaky C extensions and slowly dirtied copy on write pages, like Celery's maxtasksperchild. Staged work drains first; no task is lost to a recycle |
| `--eager-cpu` | off | Start the cpu pool at boot instead of on the first cpu task, buying the first task a warm start |
| `--no-fork-server` | off | One process per cpu task over stdio instead of the fork-server. Entered automatically if fork-server startup fails |

`--cpu-child-threads` above 1 only helps when the body **releases** the GIL,
for example a blocking network call inside a `kind="cpu"` task. For pure Python
bodies the children share nothing, so extra threads per child just re-create
the problem the process pool solved.

`--cpu-prefetch` trades latency for throughput and is not free: when a child
dies, everything staged behind it fails as retryable `WorkerLost`, and a staged
task waits out the tasks ahead of it. Raise it for small tasks, lower it for
long ones.

**The cpu pool starts on the first cpu task, not at boot.** An io only
deployment that registers cpu tasks it never calls pays nothing for them. The
first cpu task after a start waits out the pool spawn (an app import, typically
seconds); pass `--eager-cpu` when that first task's latency matters more than
the resident children.

### Delivery and safety

| Flag | Default | Effect |
|---|---|---|
| `--batch` | 16 | `XREADGROUP COUNT` per fetch. Must be at least 1 |
| `--visibility-timeout` | 60 | Seconds before a dead worker's in flight tasks are reclaimed by another. Must be at least 1 |
| `--max-envelope-bytes` | 1048576 | Oversize entries go to the DLQ as `malformed` before being parsed |
| `--drain-timeout` | 30 | Seconds to finish in flight tasks on graceful shutdown |

**`--visibility-timeout` must exceed your longest task's `timeout`** (PROTOCOL
section 4.4). The worker warns at startup when a registered task violates it.
Undersized, genuine crash recovery is slower than it needs to be, and a long
running task risks being reclaimed and run twice.

### Observability

| Flag | Default | Effect |
|---|---|---|
| `--stats-interval` | 10 | Seconds between stats log lines |
| `--log-level` | `info` | `trace`, `debug`, `info`, `warn` or `error`. `RUST_LOG` overrides |

## App object

```python
from cauli import Cauli

app = Cauli(redis_url="redis://localhost:6379/0")
```

| Option | Default | Effect |
|---|---|---|
| `redis_url` | `redis://localhost:6379/0` | Broker and result store. Falls back to `CAULI_REDIS_URL` |
| `default_queue` | `"default"` | Queue for tasks and calls that do not name one |
| `result_ttl` | 3600 | Seconds a result key survives |
| `idemp_ttl` | 86400 | Seconds an idempotency key is remembered |
| `task_routes` | `None` | `{glob: destination}`, `(pattern, destination)` pairs, or callables |
| `queue_ttl` | `None` | Seconds, or `{queue: seconds}` with `"*"` as fallback. A task picked up later is discarded |

`task_routes` overrides a task's own decorator `queue`, which is the point: an
operator re-routes without editing task code. It never overrides a per call
`queue=`, which is explicit runtime intent.

## Task decorator

```python
@app.task(kind="cpu", timeout=120, max_retries=5)
def crunch(data): ...
```

| Option | Default | Effect |
|---|---|---|
| `kind` | `"io"` | `"io"` or `"cpu"`. See [choosing a task kind](#choosing-a-task-kind) |
| `name` | `module.function` | Wire name. Must match between client and worker |
| `queue` | app default | Queue this task publishes to |
| `max_retries` | 3 | Retries before the DLQ |
| `timeout` | 300.0 | Hard timeout in seconds |
| `soft_timeout` | `None` | Seconds before `SoftTimeLimitExceeded` is raised inside the task, so it can clean up |
| `store_result` | `True` | Write `cauli:result:{id}`. `False` skips building the result entirely |
| `backoff_base` | 0.5 | First retry delay, seconds |
| `backoff_factor` | 2.0 | Multiplier per retry |
| `backoff_max` | 60.0 | Ceiling on retry delay |
| `jitter` | `True` | Randomise backoff so a fleet does not retry in lockstep |

The worker's registry is authoritative for `kind`: if an envelope disagrees
with the registered task, the registry wins.

## Per call options

```python
task.delay(*args, **kwargs)
task.apply_async(args=(), kwargs=None, countdown=None, eta=None,
                 expires=None, queue=None, idempotency_key=None)
```

| Option | Effect |
|---|---|
| `countdown` | Seconds to delay, relative |
| `eta` | Absolute datetime. **Must be timezone aware**; a naive one raises rather than being guessed at |
| `expires` | Seconds, or an absolute aware datetime. Picked up later, it is discarded with result status `expired` |
| `queue` | Overrides routing rules, the task's queue and the app default |
| `idempotency_key` | Deduplicates execution for `idemp_ttl` seconds |

`countdown` and `eta` are mutually exclusive. An `eta` in the past is not an
error, it means "due now".

Django users also get `delay_on_commit` and `apply_async_on_commit`, which
defer publishing until the current transaction commits, so a task never
observes a row that got rolled back.

## Environment variables

| Variable | Read by | Effect |
|---|---|---|
| `CAULI_REDIS_URL` | client and worker | Broker URL when not passed explicitly |
| `CAULI_LOOP` | worker | Event loop policy for the embedded asyncio loops. Unset: uvloop when importable, else stock asyncio; the startup line reports which (`impl=uvloop`). `asyncio` forces the stock loop; `uvloop` makes uvloop mandatory and fails startup without it. Force a mode when you must know which loop you measured: under a venv overlay the embedded interpreter also sees system site-packages, so uvloop can appear without being in your requirements |
| `RUST_LOG` | worker | Overrides `--log-level` |
| `VIRTUAL_ENV` | worker | The embedded interpreter calls `site.addsitedir` on this venv's site-packages. **Required when running the worker against a virtualenv**, because editable installs are invisible to a `PYTHONPATH` only interpreter |
| `CAULI_EXEC_CMD` | worker, test builds only | Overrides the cpu child command. Gated behind the `test-hooks` feature |

## Django settings

`cauli.contrib.django.django_app()` reads these from Django settings. Keyword
arguments to `django_app()` win over them, and they win over the `Cauli`
defaults.

| Setting | Maps to |
|---|---|
| `CAULI_REDIS_URL` | `redis_url` |
| `CAULI_DEFAULT_QUEUE` | `default_queue` |
| `CAULI_RESULT_TTL` | `result_ttl` |
| `CAULI_IDEMP_TTL` | `idemp_ttl` |
| `CAULI_ORM_EXECUTORS` | sticky ORM executor count for `async def` tasks, default 8. Each executor is a single thread that keeps its own DB connection, so ORM calls from async tasks reuse M connections instead of churning one per task |

`django_app()` also installs the DB connection lifecycle hooks that mirror
Celery's Django fixup: `close_old_connections` around every task, and
`connections.close_all` at process init so a connection opened at import time
never survives into a forked cpu child.

## Tuning guide

Start with `-c` alone. Change one axis at a time and measure, because the
right value depends on your task body more than on cauli.

**Processes first.** `--procs` is the largest axis the campaign found: on 4
pinned cores, 1 process to 4 was +74% throughput with lower p99, because each
process brings its own GIL *(bench3)*. `-c` scales it automatically, one
process per ~64 slots up to the cores, so a busy dedicated box fans out and a
small colocated worker stays out of your web app's way. The supervisor
restarts a dead process after 1 s and forwards SIGTERM to all of them, so
systemd and docker manage one pid as before. Each process that receives cpu
tasks starts its own cpu pool on the first one; `--cpu-workers` is divided
across procs so the child total stays at the core count.

**Sync io tasks.** `--io-threads` is the axis. Measured *(bench2)* on a two
second task with four cores: 500 threads gave 251 tasks/s, 1000 gave 495
tasks/s, and 2000 fell back to 351 tasks/s while the task body inflated from
2.00 s to 3.82 s. That inflation is the signature of oversubscription, and it
is the same shape Celery's gevent pool shows at the same point. Past the knee
you lose throughput *and* latency together.

Do not simply copy the `--io-concurrency 500` from the README quickstart onto
sync tasks. Measured cost of running wider than the optimum *(bench2)*: 9%
on an HTTP read workload, 12% on a Django ORM workload.

**Async io tasks.** `--io-concurrency` is the axis and slots are cheap, about
4 KB each. Leave `--io-loops` at 1 unless you can show otherwise: measured
*(bench2)*, 1 loop reached 1351 tasks/s against 1057, 957 and 969 for 2, 3 and
4 loops. Extra loops add threads contending for one GIL, not parallelism.

**CPU tasks.** `--cpu-workers` at the core count is the right starting point.
Sweep `--cpu-prefetch` by task size: measured *(bench2)*, 50 ms tasks were
fastest at `--cpu-prefetch 0` (60.5 against 56.0 tasks/s at the default 4),
while sub millisecond tasks want it deep.

**When none of it helps.** cauli's own dispatch measured 0.1% of worker CPU on
the sync path and 5.8% on the async path *(bench2)*. If a workload is slow and
the flags do not move it, the cost is in the task body, the database or the
service being called, and no worker setting will reach it.
