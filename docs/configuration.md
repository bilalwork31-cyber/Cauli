# Configuration

Every worker flag, app option, task option, per call option, environment
variable and Django setting, with its default. Behaviour behind each setting is
specified in [PROTOCOL.md](../PROTOCOL.md); this file says how to set it.

## Choosing a task kind

| Task body | `kind` | Runs on | Celery equivalent |
|---|---|---|---|
| `def`, waits on network or disk | `"io"` (default) | worker owned OS thread pool | `-P threads` |
| `async def` | `"io"` (default) | embedded asyncio loop | `-P gevent` |
| `def`, holds the GIL (pure Python loops, parsing, templating) | `"cpu"` | forked child processes | `-P prefork` |

The deciding question is whether the body holds the GIL. Bodies that release it
(blocking sockets, `hashlib`, `zlib`, most of numpy, Pillow) run in parallel on
the thread pool. Bodies that hold it run one at a time on a thread pool of any
width. Also pick `"cpu"` when a task must be killable at its hard timeout (a
thread cannot be killed) or leaks memory (a child is recycled).

## Worker command line

`cauli-worker -A myproj.tasks:app -c 50`

### Concurrency

| Flag | Default | Effect |
|---|---|---|
| `-c`, `--concurrency` | unset | Total tasks in flight across all worker processes. Derives the flags in the table below; an explicit flag always wins |
| `--procs` | 1; with `-c`, derived | Worker processes. The binary supervises them: spawn, restart after 1 s on death, signal fan out |
| `--print-plan` | off | Print the derived processes, threads, slots and cpu children, then exit. Needs no app and no Redis |

Derivation with `-c`. Process count rounds up, per process values round down
and are at least 1, so the fleet never exceeds `-c`: `-c 65` gives 2 processes
of 32 slots.

| Derived | Formula |
|---|---|
| `--procs` | min(cores, ⌈c / 64⌉) |
| `--io-concurrency` | ⌊c / procs⌋ |
| `--io-threads` | min(⌊min(c, 512) / procs⌋, io_concurrency) |
| `--cpu-workers` | ⌊min(cores, c) / procs⌋ |
| `--io-loops` | 1 |

Without `-c`, `--io-threads` and `--io-concurrency` keep their per process
defaults of 64 and 256 whatever `--procs` is, so `--procs 4` alone means 256
threads and 1024 io slots fleet wide. `--cpu-workers` is cores / procs. An
explicit `--io-threads` is not capped by the gate.

### Wiring

| Flag | Default | Effect |
|---|---|---|
| `-A`, `--app` | required | `module:attr`. The working directory is prepended to `sys.path` |
| `-Q`, `--queues` | `app.default_queue` | Comma separated queues. Order is dispatch order within a fetch batch |
| `--redis-url` | from app | Precedence: flag > `CAULI_REDIS_URL` > `app.redis_url` |
| `--python` | the embedded interpreter's `sys.executable` | Interpreter used to spawn cpu children |

### IO execution

| Flag | Default | Effect |
|---|---|---|
| `--io-threads` | 64, or derived | OS threads for sync `kind="io"` tasks. The scaling axis for sync work |
| `--io-concurrency` | 256, or derived | Admission gate: io tasks in flight, sync and async together. The scaling axis for async work |
| `--io-loops` | 1 | Embedded asyncio loop threads. Extra loops contend for the same GIL |

The gate is not a worker count. Above `--io-threads` it adds queueing in front
of the same threads, not sync parallelism. Size every client pool a task body
uses (redis-py, SQLAlchemy, httpx) to at least `--io-concurrency`, or to
`--io-threads` for sync tasks.

### CPU execution

| Flag | Default | Effect |
|---|---|---|
| `--cpu-workers` | cores / procs, or derived | Child processes for `kind="cpu"` tasks |
| `--cpu-child-threads` | 1 | Requests in flight per child, 1 to 1024. Helps only when the body releases the GIL |
| `--cpu-prefetch` | 4 | Requests staged in a child's socket beyond the ones it runs. 0 disables. A child death fails staged work as retryable `WorkerLost`; raise for short tasks, lower for long ones |
| `--cpu-max-tasks-per-child` | 10000 | Recycle a child after this many tasks. 0 never recycles. Staged work drains first |
| `--eager-cpu` | off | Start the cpu pool at boot instead of on the first cpu task |
| `--no-fork-server` | off | Spawn each child over stdio, one request in flight. Entered automatically if the fork server fails to start |

The recycle threshold is the only bound on a child's memory: cauli sets no
rlimit or cgroup. A child that leaks grows until the OOM killer takes it, which
surfaces as `WorkerLost` and in `cpu_lost`. Child memory shows in `cpu_rss_mb`,
never in `rss_mb`.

### Delivery and safety

| Flag | Default | Effect |
|---|---|---|
| `--batch` | 64 | `XREADGROUP COUNT` per fetch, never above the free io slots. At least 1. Fetched cpu entries wait for a cpu slot with their recovery clock running, so raise it only for short tasks |
| `--visibility-timeout` | 60 | Seconds before a dead worker's tasks are reclaimed. At least 1. A task is reclaimed only after `max(visibility_timeout, timeout + 2 s + max(redis_timeout, 2 s))` idle, so a running task is never taken over. Keep it above the longest task `timeout` anyway; nothing checks it |
| `--max-envelope-bytes` | 1048576 | Larger entries are dead lettered as `malformed`. Keep it equal to the app's `max_envelope_bytes` |
| `--drain-timeout` | 30 | Seconds to finish in flight tasks on SIGTERM or SIGINT. Tasks still running are left pending and the worker exits 88. The worker warns at startup when a task's `timeout` exceeds it |
| `--redis-timeout` | 5 | Connection and response timeout in seconds for every Redis call. A timed out call is handled like any Redis error: the task is redelivered, never lost |
| `--mover-interval` | 50 | Milliseconds between delayed and retry sweeps. Countdowns, etas, retries and beat firings start up to this late |
| `--mover-limit` | 128 | Entries moved per queue per sweep round trip. The sweep repeats until a queue is drained, so this does not cap the rate |
| `--ack-batch` | 64 | Completions buffered per queue before one flush. Values below 1 mean 1 |
| `--ack-flush-ms` | 2 | Max wait of the oldest buffered completion before a partial flush. 0 flushes every completion. A crash redelivers at most one buffer per queue |

`--redis-timeout` between 1 and half of `--visibility-timeout`. Below about 1 s,
ordinary fork, fsync and network jitter trip it. The fetch block window is
`min(1000 ms, redis_timeout / 2)` with a 50 ms floor, so a small value costs
polling, not correctness. The Python client uses the same 5 s as
`socket_timeout`.

### Observability

| Flag | Default | Effect |
|---|---|---|
| `--stats-interval` | 10 | Seconds between stats lines ([PROTOCOL.md section 7](../PROTOCOL.md#7-worker-cli-cauli-worker-binary)) |
| `--log-level` | `info` | `trace`, `debug`, `info`, `warn` or `error`. `RUST_LOG` overrides |

Alert on `async_rejected` above zero (an event loop has wedged; the process
exits 87 on its own once confirmed) and on `sync_abandoned` rising (each one is
a thread, and anything it holds, lost until restart).

## App object

`Cauli(...)` and its asyncio twin `AsyncCauli(...)`, which takes the same
options plus `async_redis_client`.

| Option | Default | Effect |
|---|---|---|
| `redis_url` | `CAULI_REDIS_URL`, else `redis://localhost:6379/0` | Broker and result store. `rediss://` works on both halves; the worker trusts the platform certificate store |
| `default_queue` | `"default"` | Queue for tasks and calls that name none |
| `result_ttl` | 3600 | Seconds a result key survives |
| `idemp_ttl` | 86400 | Seconds an idempotency key is remembered |
| `task_routes` | `None` | `{glob: destination}`, `(glob, destination)` pairs, or callables. Overrides the decorator `queue`, never a per call `queue=` |
| `queue_ttl` | `None` | Seconds, or `{queue: seconds}` with `"*"` as fallback. Measured from enqueue; a delay past it is refused at enqueue |
| `max_envelope_bytes` | 1048576 | Largest envelope the client publishes. Over it, the call raises `ValueError` and writes nothing |
| `redis_client` | `None` | A `redis.Redis`, or a zero argument factory. Overrides `redis_url` for the client and `cauli-beat`. Sentinel: `Cauli(redis_client=lambda: sentinel.master_for("mymaster"))`. The worker ignores it |
| `async_redis_client` | `None` | `AsyncCauli` only: a `redis.asyncio.Redis`, or a factory, for the async client |

## Task decorator

`@app.task(kind="cpu", timeout=120, max_retries=5)`

| Option | Default | Effect |
|---|---|---|
| `kind` | `"io"` | `"io"` or `"cpu"`. The worker's registry wins over the envelope |
| `name` | `module.qualname` | Wire name. Must match between client and worker |
| `queue` | `app.default_queue` | Queue this task publishes to |
| `max_retries` | 3 | Retries before the dead letter queue. Every exception retries |
| `timeout` | 300.0 | Hard timeout, seconds |
| `soft_timeout` | `None` | Seconds before the task is asked to stop. A `def` task gets `SoftTimeLimitExceeded` raised into its thread; an `async def` task is cancelled, so it observes `asyncio.CancelledError` and runs its `finally` blocks, and an `except SoftTimeLimitExceeded` in the body never fires. Both report `SoftTimeLimitExceeded`. Ignored unless `0 < soft_timeout < timeout` |
| `store_result` | `True` | Write `cauli:result:{id}` |
| `backoff_base` | 0.5 | First retry delay, seconds |
| `backoff_factor` | 2.0 | Multiplier per retry |
| `backoff_max` | 60.0 | Retry delay ceiling, seconds |
| `jitter` | `True` | Randomise each delay between half and all of it |

`raise cauli.Retry(countdown=...)` forces a retry with that delay, still bounded
by `max_retries`. Any exception class named `Retry` with a `countdown`
attribute counts ([PROTOCOL.md section 4.2](../PROTOCOL.md#42-retry-policy)).

## Per call options

`task.apply_async(args=(), kwargs=None, countdown=None, eta=None, expires=None, queue=None, idempotency_key=None)`

| Option | Effect |
|---|---|
| `countdown` | Seconds to delay. Exclusive with `eta` |
| `eta` | Timezone aware datetime. A naive one raises. A past one means due now |
| `expires` | Seconds, or an aware datetime. Picked up later, the task is discarded with status `expired` |
| `queue` | Overrides routes, the task's queue and the app default |
| `idempotency_key` | Suppresses a second execution under the same key for at least `idemp_ttl`. Best effort: task bodies must still be safe to repeat |

Arguments are JSON: `str`, `int`, `float`, `bool`, `None`, `list`, `tuple` and
`dict` with `str` keys. Anything else raises `TypeError` at the call site naming
the path, for example `args[1]['meta']`. A `tuple` comes back as a `list`.

`app.enqueue_many([task, (task, args), (task, args, kwargs, options)])`, and
`await app.aenqueue_many(...)` on `AsyncCauli`, publish N envelopes in one
pipelined round trip. The whole batch is validated first, so one bad call
publishes nothing. It is not a transaction: a Redis failure part way can leave
some entries written.

`task.delay_on_commit(...)` and `task.apply_async_on_commit(..., using=None)`
publish only when the Django transaction commits. Validation runs at the call
site. Under `django.test.TestCase` (pytest-django `db`) nothing is enqueued,
because the test transaction never commits: use
`self.captureOnCommitCallbacks(execute=True)` or `TransactionTestCase`
(`transactional_db`).

## Environment variables

| Variable | Read by | Effect |
|---|---|---|
| `CAULI_REDIS_URL` | client, `cauli-beat`, worker | Broker URL. For `Cauli()` an explicit `redis_url` wins. For the worker and `cauli-beat` this variable overrides the app's URL and only `--redis-url` beats it. The worker passes it to supervised processes through the environment, never argv |
| `CAULI_ALLOW_REDIS_CLUSTER` | worker | `1`, `true`, `yes` or `on` starts the worker against Redis Cluster instead of exiting 1. Delayed tasks, retries and beat lose work there |
| `CAULI_LOOP` | worker | Unset: uvloop when importable, else asyncio. `asyncio` forces the stock loop; `uvloop` fails startup without it |
| `RUST_LOG` | worker | Overrides `--log-level`. The log target is `cauli_worker_bin` for the wheel and `cauli_worker` for a `cargo build` |
| `VIRTUAL_ENV` | worker | Site packages of this venv are added to the embedded interpreter. The wheel's launcher sets it from the running venv |
| `CAULI_EXEC_CMD` | worker, `test-hooks` builds only | Replaces the cpu child command |

## Django settings

`cauli.contrib.django.django_app("myproj.settings")` returns an `AsyncCauli`.
Keyword arguments win over settings, which win over the `Cauli` defaults.

| Setting | Maps to |
|---|---|
| `CAULI_REDIS_URL` | `redis_url` |
| `CAULI_DEFAULT_QUEUE` | `default_queue` |
| `CAULI_RESULT_TTL` | `result_ttl` |
| `CAULI_IDEMP_TTL` | `idemp_ttl` |
| `CAULI_ORM_EXECUTORS` | ORM executor threads for `async def` tasks, default 8. Each keeps one connection |

It installs `close_old_connections` before and after every task and
`connections.close_all` at process init, so no connection survives a fork.

## Sizing database connections

Simultaneous Postgres connections for one worker:

```
procs * io_threads                                   (sync lane)
+ procs * min(CAULI_ORM_EXECUTORS, io_concurrency)   (async Django ORM)
+ procs * cpu_workers                                (cpu lane)
```

For `cauli.contrib.sqlalchemy` the async term is `procs * (pool_size +
max_overflow)`, 15 per process by default. Postgres ships with
`max_connections` 100. Past roughly that total, put pgbouncer in transaction
mode in front.

## Tuning

Start with `-c` alone, change one axis at a time, measure.

- **Processes.** Each process is one GIL. More `--procs` is the only way to run
  more than one GIL holding body at once. Each costs one embedded interpreter
  of memory, so pin `--procs` low when memory is the constraint.
- **Sync io.** Widen `--io-threads` while throughput rises; stop when task body
  time starts to grow.
- **Async io.** Widen `--io-concurrency`; the connection pools behind each slot
  run out before memory does.
- **CPU.** Keep `--cpu-workers` at the core count. Raise `--cpu-prefetch` for
  sub millisecond tasks; 0 can be faster for long ones.
