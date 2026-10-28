# Changelog

The format follows [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project follows [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## Unreleased

Every entry below was found by reproducing it against a real worker and
Redis, and each fix ships with a test that fails without it.

### Breaking changes

- **Cron jobs with `*` in the minute or hour field now also fire in the
  repeated hour when clocks fall back.** An every 15 minutes job used to go
  silent for 75 minutes on that night, and an hourly job skipped one run.
  Jobs at a fixed time (`30 1 * * *`) still fire once. This matches cron(8).
- **The client refuses options and payloads the worker cannot parse**, at the
  call site, with `TypeError` or `ValueError`: a non string
  `idempotency_key`, `expires` outside the representable range, negative
  `soft_timeout`, `max_retries`, `backoff_base` or `backoff_max`, a `timeout`
  that rounds to 0 ms, and arguments nested deeper than 127 levels. These
  used to be published, dead lettered as malformed, and left `get()` waiting
  forever. A cyclic payload now raises `ValueError` rather than
  `RecursionError`.
- **`autodiscover_tasks(packages=...)` raises when a listed package does not
  exist or fails to import.** It used to return without registering anything,
  and the tasks dead lettered as unregistered at run time.
- **Signature validation follows the function the worker calls**
  (`follow_wrapped=False`). A `functools.wraps` decorator that injects an
  argument no longer rejects valid `.delay()` calls; a task behind a plain
  `(*args, **kwargs)` decorator is now checked by the worker only.

### Fixed

Worker:

- A fetch never takes more entries than the process has free io slots. A
  dispatched task took its slot only when it started, so the loop fetched
  again first: with `-c 200` and 200 sleeping tasks only 100 ran (two
  processes held 100 entries each against 50 slots, two held none).
- An async loop queues a burst up to the worker's io slots instead of a
  fixed 4096: one process at `--io-concurrency 10000` failed 1,744 of 10,000
  healthy tasks with `AsyncQueueFull`.
- A dead lettered envelope over 4096 bytes (an unregistered task with large
  args) gets its failure result, so `get()` returns instead of waiting
  forever.
- Recovery no longer logs `peek (pipelined XRANGE) failed: empty command`
  at WARN on a healthy worker.
- A Redis write refused during the completion flush (OOM under
  `maxmemory-policy noeviction`) no longer acks and deletes the entry. The
  task used to vanish: no retry, no dead letter, no result. Writes now go
  first and only the requests whose writes succeeded are acked; the others
  stay pending for redelivery.
- Fatal startup errors are printed. An app import error, `--batch 0` or an
  unreachable Redis used to exit 1 with no output, because `_exit` ran
  before the log thread wrote the line.
- A stdout reader that stops reading no longer makes the worker unkillable
  except by SIGKILL; exit waits at most 1 s for queued log lines.
- An unreachable Redis at startup fails in about 17 s with the reason,
  instead of minutes of silence, and a Redis restart at run time reconnects
  within 2 s. redis-rs's default backoff jumped to its 60 s cap.
- One stop request delivered as two signals (systemd control group stop with
  `--procs`, or the supervisor dying) drains instead of forcing exit 130.
- Ctrl+C or a control group SIGTERM no longer aborts running cpu tasks
  during a graceful drain.
- A burst of cpu tasks is spread over idle children before prefetch stages
  more work on a busy one (2 tasks on 2 children: 4.05 s before, 2.05 s after).
- A cpu task whose arguments the child cannot decode is dead lettered at once
  instead of holding a slot until its hard timeout and SIGKILLing the child.
- Payload dict keys are no longer interned. On CPython 3.12 and 3.14 an
  interned string is immortal, so every distinct key leaked about 110 bytes
  for the life of the worker (1M distinct keys: 40 MB to 154 MB RSS).
- An async task that ignores its soft timeout cancellation is cancelled again
  at the hard deadline instead of running on beside its own retry.
- A `TimeoutError` an async task raises itself is reported as the task's
  error with its traceback, not as the worker's `TimeLimitExceeded`.
- A task result outside the 64 bit integer range is written exactly instead
  of dead lettering with `SerializationError`.
- A float literal outside f64 range (`1e400`) in an envelope is rejected
  instead of reaching the task as `0.0`.
- An exception message containing a lone surrogate keeps its message and
  traceback instead of both becoming empty.
- Dict key order in args, kwargs and results is preserved (serde_json
  `preserve_order`); the worker used to sort keys.
- cpu children run an interpreter that loads the worker's own libpython.
  The embedded `sys.executable` is a `PATH` lookup of `python3`, so on a uv,
  pyenv or free threaded Python the children ran a different CPython and
  could not import the app's C extensions.
- A dead letter for a malformed envelope gets a failure result whenever its
  `id` is readable, so `get()` returns instead of waiting forever.
- The Redis password is redacted from worker logs and `repr(Cauli)` for
  unix socket URLs (`?pass=`), passwords containing an unencoded `/` or `#`,
  and percent encoded query keys.

Client and integrations:

- More than 100 concurrent `.delay()` or `.adelay()` callers on one app wait
  for a free connection instead of failing with `MaxConnectionsError` (redis-py
  8 caps its default pool at 100; 300 concurrent `adelay` calls lost 200
  enqueues). The minimum redis-py is now 5.0.1.
- `AsyncCauli` keeps one Redis client per event loop. A second
  `asyncio.run()`, `async_to_sync` or a second loop thread used to fail with
  "Event loop is closed" or hang.
- `cauli.contrib.django` works with asgiref 3.13: every `async def` task
  using Django's async ORM dead lettered with `'_StickyToken' object has no
  attribute '_exited'`.
- `sqlalchemy_app()` builds its engine with `pool_size=15, max_overflow=0`:
  the same 15 connection ceiling as SQLAlchemy's default, with every
  connection kept pooled. The default closed each overflow connection on
  return, so a burst above 5 concurrent tasks paid a fresh connect per task.
  FastAPI benchmark at 500 requests/s: end to end p99 471 ms to 33 ms.
- `django_app()` and `sqlalchemy_app()` return an `AsyncCauli`, so async
  views and endpoints can `await task.adelay(...)` instead of blocking the
  event loop on Redis.

Beat:

- `on_missed="skip"` with `max_lateness` fires a slot that is on time even
  when older slots were missed.
- An entry that cannot be fired is retried once a second instead of 20 times
  a second with an ERROR line each time.

### Performance

Worker CPU per task, single process, noop tasks, interleaved A/B with a 95%
interval, 0.1.0 against this release: async 47.7 to 22.8 us (120k tasks/s),
sync 131.1 to 22.1 us (114k tasks/s), cpu 105.8 to 57.4 us. With a real task
body the body dominates: a sync task sleeping 1 ms is bound by the GIL
handing over between threads, and runs 3.9x faster on free threaded 3.14t.

- The global allocator is mimalloc: glibc malloc and its arena locks were
  about 10% of the worker's Rust threads (sync 1.16x, cpu 1.07x the rate).
- A sync pool thread takes a job off the queue only once it holds the GIL,
  with one thread on standby: each job used to wake a thread that then
  queued for the GIL behind the others (sync noop 3.63x the rate, a 1 ms
  blocking body unchanged).
- Async tasks start eagerly on their loop (Python 3.12 and newer), and every
  async deadline shares one loop timer and a heap instead of one timer per
  task (async noop 1.32x the rate together).

- Task arguments and results cross the worker as the producer's JSON text.
  The worker no longer parses them into a tree, converts that tree to Python
  objects, or writes it out again for a cpu child: msgspec, the client's
  codec, decodes and encodes them in Python. A 10,000 integer argument: async
  lane 4,123 to 643 us of CPU per task (2.44x the rate), cpu lane 4,607 to
  1,000 us (3.96x). Worker RSS with 30 in flight 1 MiB integer array
  envelopes: 2,698 to 282 MB (async), 2,665 to 297 MB (sync).
- `--batch` defaults to 64 (was 16): 4x fewer XREADGROUP round trips.
- Sync pool threads take the GIL once per burst of queued jobs, not once per
  job, so they stop convoying on it.
- The cpu admission gate wakes when the cpu backlog drains instead of
  sleeping a full 25 ms poll (`--cpu-workers 1`: 1,560 to 37,200 tasks/s on
  noop tasks).
- The async per task deadline is a loop timer instead of `asyncio.wait_for`
  on Python 3.12 and newer, with the same cancellation semantics.

### Changed

- The startup warning "task timeout_ms >= visibility_timeout" is gone. It
  fired for every task at default settings, and recovery already never
  reclaims a task that is still running. The idempotency TTL warning is gone
  too: the claim is always written with at least the task's own timeout.
- `--help` and the docs no longer describe a throughput cliff above about
  104 slots per process. It was the benchmark task modules exhausting
  redis-py's default pool of 100 connections, not the worker.
- The Celery comparison uses Celery at its best measured configuration
  (8 solo nodes, 4,233 tasks/s): cauli is 7.7x on the async lane and 4.9x on
  the sync lane, not 32.8x against a single Celery node.
- Documentation is consolidated: README, PROTOCOL (the specification), and
  `docs/` holding configuration, architecture (with the design decisions) and
  the Celery migration guide.
- The Python the worker embeds needs msgspec importable from the app's
  environment (cauli depends on it). Without it the worker exits at startup
  with an error naming that environment.
- io lane results are encoded by msgspec, as cpu lane results already were,
  so a float is written the way the client writes it (`1e16`, was `1e+16`).
  The value is the same.

### Added

- **Free threaded CPython 3.14t.** The release matrix builds, verifies and
  runs a `cp314t` worker wheel, and CI runs the client suite and the cross
  component suite on 3.14t with the GIL off. Sync tasks on threads now run in
  parallel: a CPU bound body on 10 threads went from 0.73 cores on 3.12 to
  2.87 cores on 3.14t (`python -m bench free-threaded`). 3.13t still has no wheel.
- `python -m bench web`: enqueue cost inside real Django (WSGI and ASGI) and FastAPI
  requests, end to end task latency, and an exactly once check per request.
- `python -m bench`: one entry point for the dispatch, cpu, memory, chaos, web
  and free threaded benchmarks, with Celery at its best multi node setup.
- `.gitattributes` (LF for every text file; a Windows checkout used to break
  the shell scripts), `CODE_OF_CONDUCT.md`, issue and pull request templates.

### Known limitations

- `--max-envelope-bytes` caps one envelope, not worker memory: a 1 MiB
  envelope of small integers costs up to 75 MB of RSS while it is in flight.
  PROTOCOL.md section 2 gives the sizing rule.

## 0.1.0 (2026-08-31)

First public release. Features, supported platforms, known limitations and
upgrade notes are in the
[v0.1.0 release](https://github.com/bilalwork31-cyber/Cauli/releases/tag/v0.1.0);
the full change list for this release is the
[CHANGELOG at v0.1.0](https://github.com/bilalwork31-cyber/Cauli/blob/v0.1.0/CHANGELOG.md).
