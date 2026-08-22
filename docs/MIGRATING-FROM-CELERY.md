# Migrating from Celery

This is written for someone who already runs Celery in production and wants to
know, in one sitting, what breaks. It is deliberately not a pitch. Read
"Known limitations" and "Still open" in [CHANGELOG.md](../CHANGELOG.md)
alongside it.

Three things to settle before anything else, because each one is a hard stop:

1. **The worker is Linux only.** Prebuilt wheels cover glibc 2.28 or newer on
   x86_64 and aarch64, CPython 3.10 through 3.14. macOS and Windows can
   enqueue and can run `cauli-beat`, but cannot run tasks. musl (Alpine) and
   the free threaded build have no wheel and fail the install.
2. **Redis is the only broker and the only result backend.** No RabbitMQ, no
   SQS, no database backend. Standalone only: the worker refuses to start
   against Redis Cluster, and Sentinel is reachable from the Python client and
   `cauli-beat` but not from the worker.
3. **There are no chains, groups, chords, rate limits or task priorities**, and
   none are planned for the 1.x series. If your codebase depends on canvas
   primitives, stop here.

## What ports directly

| Celery | cauli | Notes |
|---|---|---|
| `@app.task()` | `@app.task()` | Same decorator shape, same default task name of `module.function` |
| `task.delay(a, b)` | `task.delay(a, b)` | Arguments are validated against the signature at the call site and raise `TypeError` on a mismatch |
| `task.apply_async(args, kwargs, countdown=, queue=, eta=, expires=)` | same keywords | `eta` must be timezone aware. A naive datetime raises rather than being guessed at |
| `AsyncResult(id)` | `AsyncResult(id, app)` | The app is explicit, because there is no global current app |
| `result.get(timeout=10)` | `result.get(timeout=10)` | Raises `TaskFailedError` on a failed task |
| `result.status` | `result.status` | Also callable as `result.status()`. Both cost one Redis read |
| `time_limit=` | `timeout=` | Seconds. Default 300 |
| `soft_time_limit=` | `soft_timeout=` | Seconds. Ignored unless `0 < soft_timeout < timeout` |
| `SoftTimeLimitExceeded` | `cauli.SoftTimeLimitExceeded` | Read the lane note below before relying on `except` |
| `self.retry(countdown=30)` | `raise cauli.Retry(countdown=30)` | No `bind=True` needed. Still bounded by `max_retries` |
| `bind=True`, `self.request.id` | `cauli.current_task().id` | A ContextVar, correct on all three lanes. Also `.retries`, `.max_retries`, `.task`, `.queue` |
| `task_routes` | `Cauli(task_routes=...)` | Globs, `(pattern, destination)` pairs, or callables |
| `beat_schedule` | `app.add_periodic_task(name, task, schedule, ...)` | Declared in code, synced into Redis by `cauli-beat` at startup |
| `celery beat` | `cauli-beat` | Schedule state lives in Redis behind a leader lease, so two replicas fire each slot once |
| `celery -A proj worker` | `cauli-worker -A proj.tasks:app -c 50` | `-A` takes `module:attr`, not a module |
| `worker_process_init` signal | `@app.process_init` | Runs once per cauli managed Python process |
| `task_prerun` / `task_postrun` | `@app.before_task` / `@app.after_task` | Called with no arguments. A raising hook is logged and skipped, never fails the task |
| `django-celery-results`, the Celery fixup | `cauli.contrib.django` | `django_app("myproj.settings")`, connection hooks, `delay_on_commit` |
| `-P prefork` | `@app.task(kind="cpu")` | Per task, not per worker. All three lanes run in one worker process |
| `-P threads` | `def` task, the default | |
| `-P gevent` | `async def` task, the default | Real asyncio, not monkey patching |

## The eight divergences that bite silently

These are the ones that do not raise. They change behaviour and you find out
in production.

**1. `-c` counts tasks, not processes.** Celery's `-c 50` with prefork forks 50
processes. cauli's `-c 50` is 50 tasks in flight, and it derives the process
count, thread count and admission gate from that. A Celery operator who copies
their `-c` across will get far more concurrency than they expect, and a
database connection count to match. `cauli-worker --print-plan` shows the
derivation without Redis and without your app. Run it first.

**2. `crontab()` field order is `cron(8)`'s, not Celery's.** cauli takes
minute, hour, day_of_month, month, day_of_week. Celery takes minute, hour,
day_of_week, day_of_month, month_of_year. The third and fourth arguments are
swapped. Three or more positional arguments now raise `TypeError` rather than
build a different schedule, and `month_of_year=` raises a message naming
cauli's `month=`. Two positional arguments, `crontab(0, 4)`, mean the same
thing in both.

**3. `crontab()` ORs day_of_month and day_of_week.** When both are restricted,
`cron(8)` fires on either, and so does cauli.
`crontab(minute=0, hour=0, day_of_month=1, day_of_week=1)` fires on the 1st of
the month **and** on every Monday. Celery ANDs them, so the same expression
fires only on a Monday that is also the 1st. This is the divergence most likely
to produce a schedule that looks right and is not.

**4. `soft_timeout` reaches an `async def` task as a cancellation.** A `def`
task gets `SoftTimeLimitExceeded` raised into its thread, so
`except SoftTimeLimitExceeded` works the way it does under Celery. An
`async def` task is cancelled at the soft mark instead: it observes
`asyncio.CancelledError`, its `finally` blocks run, and an
`except SoftTimeLimitExceeded` inside the body never fires. The failure cauli
reports is `SoftTimeLimitExceeded` either way, so callers and the dead letter
queue agree. Port cleanup into `finally`, not into `except`.

**5. Retries are automatic and always backed off.** There is no
`autoretry_for` and no `retry_backoff` flag, because there is nothing to turn
on: any exception retries up to `max_retries` (default 3) with exponential
backoff and jitter, controlled by `backoff_base`, `backoff_factor`,
`backoff_max` and `jitter` on the decorator. `raise cauli.Retry(countdown=X)`
forces an explicit delay. A Celery task that relied on the default of **no**
retry will now retry three times.

**6. Delivery is at least once, always.** There is no `acks_late=False`
equivalent that turns it into at most once. A crashed worker's in flight tasks
are redelivered. Worst case executions of one task are
`(max_retries + 1) x (redelivery_limit + 1)`, which is 20 with the defaults.
Task bodies must be safe to repeat. `idempotency_key=` suppresses a duplicate
enqueue, but it is a 64 bit fold and a collision suppresses a distinct task, so
it is a throughput saver, not a correctness mechanism.

**7. Arguments are JSON only, and the type set is smaller than you think.**
Allowed: `str`, `int`, `float`, `bool`, `None`, `list`, `tuple`, and `dict`
with `str` keys. A `UUID`, a `datetime`, a `Decimal` or a model instance raises
`TypeError` at the call site, naming the path to the offending argument. There
is no `serializer=` option and no pickle. A `tuple` encodes as a JSON array and
comes back as a `list`, so a round trip is not type preserving. Django users:
a `UUIDField` primary key is the usual first casualty. Pass `str(obj.pk)`.

**8. `delay_on_commit` never fires under `django.test.TestCase`.** This is true
of Celery's `delay_on_commit` too, and it catches people in both. The test runs
inside an atomic block that always rolls back, so the on commit callback never
runs and the assertion fails with nothing enqueued. Use
`self.captureOnCommitCallbacks(execute=True)` or subclass
`TransactionTestCase`. With pytest-django, the `db` fixture behaves like
`TestCase` and `transactional_db` behaves like `TransactionTestCase`.

## What cauli does not have

Stated plainly, so you can decide before you port anything.

| Celery feature | Status in cauli 1.x |
|---|---|
| `chain`, `group`, `chord`, `map`, `starmap`, signatures | Not implemented, not planned |
| Task priorities | Not implemented, not planned |
| Rate limits (`rate_limit=`) | Not implemented, not planned |
| `celery inspect`, `celery purge`, `celery control` | No equivalent. `cauli-beat` is the only console script besides the worker |
| Flower or any dashboard | None. The operator interface is a stats log line every `--stats-interval` seconds |
| A metrics endpoint, JSON logging, a health endpoint | All rejected for 1.0. Scraping means parsing a log line |
| Brokers other than Redis | Not planned |
| Result backends other than Redis | Not planned |
| Pickle or a custom serializer | Not planned. JSON only, by design |
| `task_always_eager` | No setting, but a task stays a plain callable: `crunch([1, 2])` runs inline with no broker |
| Dead letter tooling | The dead letter queue exists and holds roughly the most recent 1000 entries per queue. Nothing reads, exports, requeues or purges it for you |
| Push based result notification | `get()` polls every 50ms by default. `poll_interval` is tunable per call |

## A migration order that works

1. Run `cauli-worker --print-plan -c <your concurrency>` and read the derived
   thread and process counts against your database's `max_connections`. The
   connection arithmetic is in
   [docs/CONFIGURATION.md](CONFIGURATION.md). This is the step
   people skip and then blame on cauli.
2. Grep for `crontab(` and check every call with three or more positional
   arguments, and every schedule that restricts both a day of month and a day
   of week. These are divergences 2 and 3, and neither raises today under
   Celery.
3. Grep for `UUID`, `datetime`, `Decimal` and model instances in `.delay()` and
   `.apply_async()` arguments. These now raise at the call site.
4. Grep for `except SoftTimeLimitExceeded` inside `async def` bodies and move
   the cleanup into `finally`.
5. Check every task that has no explicit `max_retries`. It now retries three
   times where Celery did not retry at all.
6. Port one low stakes queue first and run both workers side by side. The
   envelope format is frozen for 1.x
   ([PROTOCOL.md](../PROTOCOL.md)), but the queues are separate, so there is no
   shared state to corrupt while you do it.
