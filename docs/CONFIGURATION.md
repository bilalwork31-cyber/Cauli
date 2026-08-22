# Configuration reference

Every knob cauli has: the worker command line, the app object, the task
decorator, per call options, environment variables, and the Django settings
the contrib layer reads.

Where a default was chosen for a reason, the reason is stated. None of that
reasoning is a published benchmark result: this file used to carry figures from
a campaign whose harness is no longer in the tree, and those figures have been
removed rather than republished without one. Reproducible numbers live in
`bench/`, starting with `bench/CLAIMS.md` for what is actually tested and
`bench/RESULTS.md` for what is not. Treat the guidance below as design
rationale to confirm on your own hardware, not as a promise about it. Before
citing a figure from `bench/` anywhere, check that the lane's task module is
tracked: `git ls-files bench/`. A claim whose harness is not in the tree is
not a claim this project can defend.

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

The difference is not small. A thread pool gives real concurrency on GIL
releasing work and none at all on GIL holding work, where per task time
inflates roughly in step with how many bodies are queued behind the one
holding the GIL. That inflation is the GIL serialising the bodies, and it is
why "CPU bound" on its own is not enough information to pick a kind.

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
wins. Every division below is ceiling division, `⌈a / b⌉`: it rounds up, so
`-c 65` gives 2 processes and `-c 200` gives 4, not the 1 and 3 a floor
reading predicts. The rules, and the reasoning behind each:

| Derived | Formula | Why |
|---|---|---|
| `--procs` | min(cores, ⌈c / 64⌉) | Each process is one GIL: fanning out is the only way to run more than one GIL holding body at a time, and it lowers p99 as well as raising throughput. Scaling by `-c` keeps a small queue worker to one quiet process on a box shared with your web app and Redis, while a large `-c` on a dedicated box uses every core. The 64 slot target is a chosen default, not a swept one |
| `--io-concurrency` | ⌈c / procs⌉ | The gate is the real bound for `async def` tasks; a slot costs about 4 KB |
| `--io-threads` | ⌈min(c, 512) / procs⌉, at most the gate in this derivation | Thread pools stop paying somewhere around a thousand threads per process; past that, throughput and latency fall together. Staying at 1x the gate is the latency honest default: oversubscribing buys throughput by inflating task p99. The gate gets no special enforcement of its own: an explicit `--io-threads` is not capped by it, so `-c 50 --io-threads 999` really does give 999 threads against a gate of 50 |
| `--cpu-workers` | ⌈min(cores, c) / procs⌉ | More cpu children than cores buys nothing, and more than `-c` would make `-c 8` on a cpu queue mean something other than 8 |
| `--io-loops` | always 1 | Extra loops add threads that contend for one GIL, not parallelism |

Without `-c`, one process and the standalone defaults below apply, unless
`--procs` is passed alone: it still divides `cpu_workers` across the
processes you asked for, but `--io-threads` and `--io-concurrency` stay at
their flat per process defaults, 64 and 256. `--procs 4` alone therefore
means 4x those defaults fleet wide, 256 threads and 1024 io slots, not 64
and 256 split four ways. Pass `-c`, which divides both, to scale them down
instead.

### Wiring

| Flag | Default | Effect |
|---|---|---|
| `-A`, `--app` | *required* | App location as `module:attr`. The worker's working directory is prepended to `sys.path`, so relative module paths resolve |
| `-Q`, `--queues` | `app.default_queue` | Comma separated queue names to consume |
| `--redis-url` | from app | Precedence: this flag > `CAULI_REDIS_URL` > `app.redis_url` |
| `--python` | the embedded interpreter | Interpreter used to spawn cpu children. Unset, the worker asks its own embedded CPython for `sys.executable`, so a non activated venv no longer silently resolves to the system `python3` without the `cauli` package. Pass the flag to override; a warning names the fallback if neither can be resolved |

### IO execution

| Flag | Default | Effect |
|---|---|---|
| `--io-threads` | 64, or derived from `-c` | OS threads for sync `kind="io"` tasks. **This is the scaling axis for sync work** |
| `--io-concurrency` | 256, or derived from `-c` | Admission semaphore: maximum io tasks in flight, sync and async together. Above about 128 per process is untested; see the band note below |
| `--io-loops` | 1 | Embedded asyncio event loop threads for `async def` tasks |

`--io-concurrency` is a gate, not a worker count. Raising it above
`--io-threads` does not add sync parallelism, it only lets more tasks queue
behind the same threads. With `--io-threads 500` a two second body is capped
near 250 tasks/s whatever the gate says, so raising the gate from 500 to 4000
buys no sync throughput, only a longer queue in front of the same threads.

For `async def` tasks the gate *is* the bound, because a slot costs a coroutine
and a socket rather than a thread.

**The default of 256 is higher than anything this project has confirmed
works, and it is deliberate that the two do not agree.** Say so plainly rather
than let a reader find it:

- `bench/RESULTS.md` (the async lane at `--procs 6`) reports the per process
  gate peaking near 96, then runs that finish 91% to 99% and stall above 104,
  down to 11% of a run completed at 512. That harness is in the tree and is
  rerunnable.
- The stall is not attributed. The same harness opens one Redis connection per
  concurrent caller and pins no `max_connections`, so at those settings it may
  have run the Redis server out of client slots rather than found a limit in
  cauli. Until it is rerun with connections pinned, neither reading is proven.
- The default was left at 256 rather than lowered onto an unattributed number.
  That is a judgement call, not a measurement, and it is the open item this
  section exists to flag.

What to do with that until it is settled:

- Treat anything above 128 slots per process as untested rather than supported.
- Expect a wall that looks like a hang, not a smooth decline, if you go past it.
- Pin `--io-concurrency` yourself if you would rather not sit on an untested
  default. Anything in the 64 to 128 band per process is inside what has run.
- Remember each async slot can hold a database connection, so the gate feeds
  the connection formula in the sizing section below.

### CPU execution

| Flag | Default | Effect |
|---|---|---|
| `--cpu-workers` | ⌈min(cores, c) / procs⌉ | Child processes for `kind="cpu"` tasks |
| `--cpu-child-threads` | 1 | Requests pipelined per child, matched by id. Range 1 to 1024 |
| `--cpu-prefetch` | 4 | Requests staged in a child's socket buffer beyond the one it is running. 0 disables |
| `--cpu-max-tasks-per-child` | 1000 | Recycle a child after this many completed tasks; 0 disables recycling. The backstop for leaky C extensions and slowly dirtied copy on write pages, like Celery's maxtasksperchild. Staged work drains first; no task is lost to a recycle |
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

**`rss_mb` in the worker's stats line is the worker process only; it does not
include cpu children.** Each forked child is a separate process with its own
memory, never summed into that number. `--cpu-max-tasks-per-child` (default 1000;
set it to 0 to disable recycling) is the ONLY mechanism that bounds a child's
memory: cauli sets no
rlimit and no cgroup on it. A task with a real leak, or one that just holds a
large result a moment too long, grows that child until the OS OOM killer takes
it, and that death surfaces as a generic `WorkerLost` like any other. A child
can grow to hundreds of megabytes while `rss_mb` never moves. If cpu tasks are
memory hungry, set a recycle threshold; do not rely on the stats line to
notice for you.

**The cpu pool starts on the first cpu task, not at boot.** An io only
deployment that registers cpu tasks it never calls pays nothing for them. The
first cpu task after a start waits out the pool spawn (an app import, typically
seconds); pass `--eager-cpu` when that first task's latency matters more than
the resident children.

### Delivery and safety

| Flag | Default | Effect |
|---|---|---|
| `--batch` | 16 | `XREADGROUP COUNT` per fetch. Must be at least 1 |
| `--visibility-timeout` | 60 | Seconds before a dead worker's in flight tasks are reclaimed by another. Must be at least 1. This is a floor, not the whole rule: recovery waits for `max(visibility_timeout, task timeout + grace + margin)`, where the margin is sized on `--redis-timeout`, so a task never gets reclaimed while it is still legitimately running |
| `--max-envelope-bytes` | 1048576 | Oversize entries go to the DLQ as `malformed` before being parsed. Enforced on both ends: the client refuses first, so keep this equal to the app's `max_envelope_bytes`. A client limit above the worker's produces envelopes the worker dead letters, and past 4096 bytes it cannot recover the task id, so no failure result is written and `get()` waits out its own timeout |
| `--drain-timeout` | 30 | Seconds to finish in flight tasks on graceful shutdown |
| `--redis-timeout` | 5 | Response and connection timeout, in seconds, for every redis round trip. Forwarded to every supervised worker process |
| `--mover-interval` | 250 | Milliseconds between delayed and retry sweeps (PROTOCOL section 4.3) |
| `--mover-limit` | 128 | Entries the sweep moves per queue per round trip. The sweep repeats within one tick until a queue comes back short, so this bounds one `EVAL`, not the drain rate |

**`--visibility-timeout` must exceed your longest task's `timeout`** (PROTOCOL
section 4.4). The worker warns at startup when a registered task violates it.
Undersized, genuine crash recovery is slower than it needs to be, and a long
running task risks being reclaimed and run twice.

**`--redis-timeout` bounds fetch, the idempotency claim, the delayed mover,
crash recovery, and result writes**, all of which otherwise wait on redis with
no client side deadline at all. Without it, a redis that accepts the TCP
connection but never answers (paused, swapping, or a network partition
dropping packets rather than refusing them) hangs the affected call forever;
`BLOCK` on `XREADGROUP` is a server side wait, not a client side one, and does
not help. There is no single correct default: it depends on this deployment's
redis tail latency. Below roughly 1 second, ordinary fork, fsync and network
jitter risk a false trip, including the delayed mover's Lua script, which can
touch up to 128 items in one round trip. Past roughly half of
`--visibility-timeout`, a slow but genuinely alive redis is not caught
meaningfully sooner than doing nothing. A trip is never a new failure mode:
every affected call site already has a tested fallback for a redis error
(a failed result write leaves the entry unacked for `XCLAIM` to redeliver, the
idempotency claim fails open and executes anyway), so this only reaches an
existing safe outcome sooner, at the cost of one log line and at most one
`--visibility-timeout` of added latency on that task. Never data loss.

The Python client built by `Cauli._get_redis()` (`py/cauli/app.py`) passes the
same 5 second default as `socket_timeout` explicitly, for the same reason:
redis-py's own default depends on the installed version.

**Do not set `--redis-timeout 1`.** The fetch loop blocks on `XREADGROUP` for
up to 1000 ms on an empty queue, and the same deadline applies to that call, so
the two race on scheduling jitter and every lost race tears the connection
down. The worker now derives its block window as
`min(1000 ms, --redis-timeout / 2)` with a 50 ms floor and logs the shortened
window at `info`, so a value of 1 costs you a 500 ms poll rather than a
reconnect storm. It is still the wrong lever: raise `--redis-timeout` to match
your redis tail latency instead of lowering it to force faster polling.

**The delayed and retry sweep is no longer rate capped.** Before this release
the sweep moved at most `--mover-limit` entries per queue per tick with no
retry, which fixed the drain rate at 512 per second per queue per process
whatever the flags said. Every retry, `countdown` and `eta` passes through that
path, so a high retry rate grew `cauli:delayed:{queue}` without bound and
nothing in the stats line watched it. The sweep now repeats within a tick until
a queue returns a short page, up to 32 rounds. No published figure exists for
the ceiling that leaves; `bench/` has no retry rate lane yet.

### Observability

| Flag | Default | Effect |
|---|---|---|
| `--stats-interval` | 10 | Seconds between stats log lines |
| `--log-level` | `info` | `trace`, `debug`, `info`, `warn` or `error`. `RUST_LOG` overrides |

**What to alert on.** Two fields in the stats line (PROTOCOL section 7) mean
something is already broken, not that a threshold is near:

- **`async_rejected` above zero.** The shim's per loop submission queue
  rejects only once it is at its cap, and it only reaches that cap when an
  event loop thread has wedged. That process's async lane does not recover on
  its own; restarting the process is the only fix.
- **`sync_abandoned` climbing.** Each abandonment is one sync io task that
  overran its hard timeout and kept running anyway. The thread is never
  reclaimed, and neither is anything it holds, a database connection
  included, so a rising counter is capacity lost for good.

Set `--cpu-max-tasks-per-child` to a nonzero value in production. It is the
only bound on a cpu child's memory, and `rss_mb` cannot see that growth (see
CPU execution above).

## App object

```python
from cauli import Cauli

app = Cauli(redis_url="redis://localhost:6379/0")
```

| Option | Default | Effect |
|---|---|---|
| `redis_url` | `redis://localhost:6379/0` | Broker and result store. Falls back to `CAULI_REDIS_URL`. `rediss://` is supported by both halves: the worker links rustls and trusts the platform certificate store, so Upstash, Azure Cache and ElastiCache with encryption in transit connect without a build flag |
| `default_queue` | `"default"` | Queue for tasks and calls that do not name one |
| `result_ttl` | 3600 | Seconds a result key survives |
| `idemp_ttl` | 86400 | Seconds an idempotency key is remembered |
| `task_routes` | `None` | `{glob: destination}`, `(pattern, destination)` pairs, or callables |
| `queue_ttl` | `None` | Seconds, or `{queue: seconds}` with `"*"` as fallback. A task picked up later is discarded. Measured from enqueue, never from due time: a `countdown` or `eta` past the TTL is refused at enqueue rather than discarded unrun later |
| `max_envelope_bytes` | 1048576 | Largest encoded envelope `.delay()`, `.apply_async()` and the batch calls will publish. Over it they raise `ValueError` naming the task, the measured size and the limit, and write nothing. Keep it equal to the worker's `--max-envelope-bytes`. `cauli-beat` does not read it |
| `redis_client` | `None` | A ready `redis.Redis`, or a zero argument factory returning one. Overrides `redis_url` for the Python client and `cauli-beat`. This is the Sentinel injection point: `Cauli(redis_client=lambda: sentinel.master_for("mymaster"))`. The Rust worker does not read it and connects by URL |

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
| `soft_timeout` | `None` | Seconds before the task is asked to stop early, so it can clean up. The hard `timeout` stays the backstop behind it. **The two io lanes signal it differently**: a `def` task receives `SoftTimeLimitExceeded` raised into its thread, so `except SoftTimeLimitExceeded` works; an `async def` task is cancelled at the soft mark, so it observes `asyncio.CancelledError` and its `finally` blocks run, while an `except SoftTimeLimitExceeded` inside the body never fires. The failure cauli reports is `SoftTimeLimitExceeded` on both, so callers, the DLQ and the result document agree. Ignored unless `0 < soft_timeout < timeout` |
| `store_result` | `True` | Write `cauli:result:{id}`. `False` skips building the result entirely |
| `backoff_base` | 0.5 | First retry delay, seconds |
| `backoff_factor` | 2.0 | Multiplier per retry |
| `backoff_max` | 60.0 | Ceiling on retry delay |
| `jitter` | `True` | Randomise backoff so a fleet does not retry in lockstep |

The worker's registry is authoritative for `kind`: if an envelope disagrees
with the registered task, the registry wins.

Raising `cauli.Retry(countdown=...)` inside a task forces a retry with that
delay instead of the computed backoff, still bounded by `max_retries`. The
worker recognises it by class name plus a `countdown` attribute rather than by
class identity, so an exception class of your own named `Retry` that carries a
`countdown` is read the same way. Identity matching is not available here: the
worker's interpreter is not required to have cauli installed at all, and the cpu
lane decides in Rust from a type name read off a pipe. The collision costs
little, since cauli retries every exception by default anyway; the only
difference is that your `countdown` replaces the computed backoff, and the task
still retries `max_retries` times and still dead letters with your own type and
traceback.

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
| `idempotency_key` | Deduplicates execution for `idemp_ttl` seconds. It narrows the duplicate window, it does not close it: the task body still has to be safe to repeat. See PROTOCOL.md section 4, "Delivery guarantee" |

`countdown` and `eta` are mutually exclusive. An `eta` in the past is not an
error, it means "due now".

### Argument types

Arguments and keyword arguments are encoded as JSON. The accepted set is
`str`, `int`, `float`, `bool`, `None`, `list`, `tuple`, and `dict` with `str`
keys. Anything else raises `TypeError` at the call site, naming the path to
the offending value, for example `args[1]['meta']`. There is no `serializer=`
option and no pickle path. Note that a `tuple` encodes as a JSON array and
decodes back as a `list`, so a round trip is not type preserving. The usual
first casualties porting from Celery are `UUID`, `datetime`, `Decimal` and
model instances.

### Batching

```python
app.enqueue_many([
    send_email,                                    # no arguments
    (send_email, ("a@b.com",)),                    # args
    (crunch, ([1, 2],), {}, {"queue": "bulk"}),    # args, kwargs, options
])
await app.aenqueue_many([...])                     # on AsyncCauli
```

N envelopes in one pipelined round trip instead of N. `options` accepts
exactly the `apply_async` keywords and rejects anything else by name. The
whole batch is validated and encoded before the first write, so one bad or
oversize call aborts the batch with nothing published. The pipeline is not a
transaction: a Redis failure part way through can leave some entries written.

### On commit, for Django

Django users also get `delay_on_commit` and `apply_async_on_commit`, which
defer publishing until the current transaction commits, so a task never
observes a row that got rolled back. Validation is not deferred with it: the
signature check and the JSON encode dry run run at the call site, inside your
`atomic()` block, so a bad argument rolls the transaction back instead of
raising at COMMIT with the row already written.

**Nothing is enqueued under `django.test.TestCase`.** That class wraps every
test in an atomic block it always rolls back, so the on commit callback never
runs. Wrap the assertion in `self.captureOnCommitCallbacks(execute=True)`, or
subclass `TransactionTestCase`. With pytest-django, the `db` fixture behaves
like `TestCase` and `transactional_db` behaves like `TransactionTestCase`.

## Environment variables

| Variable | Read by | Effect |
|---|---|---|
| `CAULI_REDIS_URL` | client, `cauli-beat` and worker | Broker URL. **Precedence differs by consumer.** For `Cauli(redis_url=...)` an explicit argument wins over this variable. For `cauli-beat` and the worker this variable overwrites the URL the app declared in code, and only `--redis-url` beats it. A stale value in a beat or worker container therefore silently redirects that process while the web app keeps enqueueing elsewhere. The worker also reads it in preference to argv so a password never reaches `/proc/<pid>/cmdline`, and the supervisor passes it to its children the same way |
| `CAULI_ALLOW_REDIS_CLUSTER` | worker | Set to `1` to start against a Redis running in cluster mode instead of exiting 1. Off by default: the worker sends `INFO cluster` before it touches a consumer group and refuses when the reply says `cluster_enabled:1`, because `cauli:q:{queue}` and `cauli:delayed:{queue}` never share a hash slot, so a delayed or retried task leaves the stream without ever reaching the delayed set. That is silent loss, not a visible error. A Redis that will not answer the probe at all starts normally |
| `CAULI_LOOP` | worker | Event loop policy for the embedded asyncio loops. Unset: uvloop when importable, else stock asyncio; the startup line reports which (`impl=uvloop`). `asyncio` forces the stock loop; `uvloop` makes uvloop mandatory and fails startup without it. Force a mode when you must know which loop you measured: under a venv overlay the embedded interpreter also sees system site-packages, so uvloop can appear without being in your requirements |
| `RUST_LOG` | worker | Overrides `--log-level`. A bare level such as `RUST_LOG=debug` works everywhere. A per target directive has to name the right target, and that differs between builds: the wheel ships `cauli_worker_bin`, while a local `cargo build` produces `cauli_worker`. So use `RUST_LOG=cauli_worker_bin=debug` against an installed worker and `RUST_LOG=cauli_worker=debug` against one you built yourself. Both binaries are the same `src/main.rs`; the two names exist so that `cargo build` keeps producing `cauli-worker` for the integration suite while the wheel ships its console script wrapper under that name |
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

**Connection count.** Those hooks manage connection staleness, not
connection count. Each worker process opens its own pool, and each sync
thread keeps one Django connection for its life, so the number of
simultaneous Postgres backends is:

```
procs * io_threads                                  (sync lane)
+ procs * min(CAULI_ORM_EXECUTORS, io_concurrency)   (async lane)
+ procs * cpu_workers                                (cpu lane; each forked child connects on its own)
```

When `--io-threads` is derived from `-c` the total stays near `min(c, 512)`
automatically (see Worker command line above). Without `-c`, or with
`--io-threads` set explicitly, that cap does not apply and the sync lane is
a plain product: `--procs 4` alone leaves `io_threads` at its standalone
default of 64 per process, for 4 * 64 = 256 connections, and
`--procs 4 --io-threads 30` explicitly is 4 * 30 = 120. Postgres ships with
`max_connections` at 100, about 97 usable, so totals that look modest at
the flag level can still exhaust it. Put a connection pooler, pgbouncer in
transaction mode, in front of Postgres once the total climbs past roughly
a hundred. `bench/RESULTS.md` (Claim 5) has a measured exhaustion and the
pooler recovery.

## Tuning guide

Start with `-c` alone. Change one axis at a time and measure, because the
right value depends on your task body more than on cauli.

**Processes first.** `--procs` is the largest axis cauli has, because each
process brings its own GIL and one process runs a single GIL holding body at a
time however wide its pools are. `-c` scales it automatically, one process per
~64 slots up to the cores, so a busy dedicated box fans out and a small
colocated worker stays out of your web app's way. The supervisor
restarts a dead process after 1 s and forwards SIGTERM to all of them, so
systemd and docker manage one pid as before. Each process that receives cpu
tasks starts its own cpu pool on the first one; `--cpu-workers` is divided
across procs so the child total stays at the core count.

**Sync io tasks.** `--io-threads` is the axis. Widen it while throughput still
rises, and stop when the task body starts inflating: a body that takes longer
under a wider pool than it did under a narrow one is the signature of
oversubscription, the same shape Celery's gevent pool shows at the same point.
Past that knee you lose throughput *and* latency together, so the last useful
value is the one just before the body time moves.

Do not simply copy the `--io-concurrency 500` from the README quickstart onto
sync tasks. Running wider than the optimum is not free: past the knee you pay
in both throughput and per task latency, so find the knee on your own workload
rather than starting above it.

**Async io tasks.** `--io-concurrency` is the axis and a slot itself is cheap,
about 4 KB, but see the band note above before you climb past 128 per process:
the memory is not what runs out first. The connections each slot can hold are,
and above 128 you are past everything this project has run. Leave `--io-loops` at 1 unless you can show otherwise on your own
workload: extra loops add threads that contend for one GIL, not parallelism.

**CPU tasks.** `--cpu-workers` at the core count is the right starting point.
Sweep `--cpu-prefetch` by task size. Deep prefetch pays for sub millisecond
tasks, where the round trip dominates the body. It stops paying once the body
is long enough to hide the fetch, and `--cpu-prefetch 0` can then be faster,
because a prefetched entry waits in one child's queue instead of on a free one.

**When none of it helps.** cauli's own dispatch is a thin layer: a Redis read,
an envelope decode and a handoff into a pool. Nothing in that path is designed
to be the bottleneck, but no figure for its share of worker CPU is published
here, because none has been reproduced by a harness in this tree. If a workload
is slow and the flags do not move it, look in the task body, the database or
the service being called; a worker setting is unlikely to reach it.
