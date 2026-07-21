# cauli-worker architecture (as built)

One Rust OS process (`cauli-worker`) executes many Python tasks concurrently
against Redis Streams per PROTOCOL.md. Module map:

| File | Role |
|---|---|
| `src/main.rs` | Bootstrap: CLI, tracing, Python init, redis connect, spawns all loops, §4.7 drain, exit codes |
| `src/cli.rs` | clap CLI per §7 + queue name validation |
| `src/loops.rs` | fetch loop (XREADGROUP), delayed mover (§4.3), recovery (§4.4), stats |
| `src/dispatch.rs` | per-entry pipeline: parse -> idempotency (§4.5) -> route -> finish (§4.1/§4.2) |
| `src/exec.rs` | sync-thread / async-loop / cpu-child execution, timeouts (§4.6) |
| `src/ctx.rs` | shared context, executor response -> Outcome normalization |
| `src/pyrt.rs` | pyo3 glue: interpreter init, shim import, sync io thread pool, async submit bridge |
| `src/shim.py` | embedded Python shim (include_str!): app loading, run_sync, asyncio loops, soft-timeout watchdog |
| `src/cpu.rs` | §5.1 cpu pool: fork-server (unix socket, multiplexed) + stdio fallback, kill+respawn, `CAULI_EXEC_CMD` test hook |
| `src/broker.rs` | Redis key layout, group setup, mover Lua, pipelined completion writes |
| `src/envelope.rs` | §2 envelope (unknown-field preserving), §8 result/error JSON, redelivery limit |
| `src/backoff.rs` | §4.2 backoff math (jitter after clamp) |
| `src/stats.rs` | counters + `stats:` line, rss_mb from /proc/self/status VmRSS |

## Threading model

- **tokio multi-thread runtime** owns all broker plumbing: one fetch loop doing
  `XREADGROUP ... BLOCK 1000` over all queues (dedicated ConnectionManager so
  BLOCK never stalls writes), pipelined ack/retry/DLQ writers, 250ms delayed
  mover (single EVAL per queue), XPENDING/XCLAIM recovery every
  visibility_timeout/2, stats loop, signal task.
- **Embedded CPython** via pyo3 auto-initialize; `prepare_freethreaded_python()`
  at startup before the runtime. The GIL is NEVER held on tokio worker threads
  during broker I/O: sync tasks run on dedicated OS threads, async submission
  happens inside `spawn_blocking`, completions arrive from Python loop threads.
- **Shim** (`src/shim.py`, imported once via `PyModule::from_code`): the pyo3
  surface is strings-in/strings-out. `load_app` duck-types (getattr only) the
  app per §6 and honors VIRTUAL_ENV site-packages; `run_sync` captures returns/
  exceptions/Retry/SerializationError as outcome JSON; `start_loops(N)` spawns
  N daemon threads each running `loop.run_forever()`; `submit_async` uses
  `run_coroutine_threadsafe` + `asyncio.wait_for(coro, effective_s)` and pushes
  completion into a Rust `PyCFunction` closure -> tokio oneshot (no polling).
  Soft timeouts for sync tasks are serviced by ONE shared watchdog thread (a
  min-heap of (deadline, tid, generation)) rather than a `threading.Timer` per
  call, so hot-path soft-timeout usage does not spawn thousands of OS threads
  per second. The Rust side gives up waiting on an async completion after
  `timeout_ms + grace` and drops its pending-map slot (`PyRuntime::cancel`) so
  a wedged event-loop thread that never actually finishes the coroutine
  cannot leak that bookkeeping forever (the coroutine itself, if truly
  wedged on a synchronous blocking call, cannot be recovered from Rust --
  `pending_async` in the stats line makes a growing count of these visible).
- **Sync io pool** (`--io-threads`): Rust-spawned OS threads; each takes the
  GIL only around the shim `run_sync` call; CPython releases it during blocking
  I/O, so io parallelism is real. Soft timeout: shim watchdog `threading.Timer`
  injecting `SoftTimeLimitExceeded` via `PyThreadState_SetAsyncExc`, fenced by
  a per-thread generation counter so a timer that fires late (after its own
  task already finished) cannot land inside a later task on the same thread.
  The job queue is a bounded crossbeam channel (capacity = `--io-concurrency`);
  a queued job whose dispatcher already hard-timed-out is skipped rather than
  executed late ("zombie execution") when a thread reaches it, and a hard
  timeout spawns a replacement thread immediately so pool capacity is
  restored even though the wedged original thread can never be killed.
- **Cpu pool** (`--cpu-workers`, `--cpu-child-threads`): fork-server mode by
  default (§5.1). ONE parent (`{python} -m cauli._exec --app {spec}
  --fork-server --connect {sock} --child-threads {M}`) imports the app once,
  `gc.collect()` + `gc.freeze()`, then forks a child per `{"cmd":"fork"}`
  control line; children connect to the worker's tokio `UnixListener`, send
  `{"ready": true, "pid", "concurrency"}` and serve the line protocol on the
  socket. Per child, one `serve_child` task multiplexes up to `concurrency`
  requests (pending map keyed by wire id, per-request hard-timeout deadlines
  via a single earliest-deadline sleep in the select loop). Hard timeout:
  SIGKILL by pid, expired requests fail "TimeoutError", the rest "WorkerLost"
  (both retryable), then a replacement fork (cheap: no re-import). Child
  death (socket EOF): all in flight "WorkerLost" + replacement fork. Parent
  control-channel failure mid-run: parent respawned with 1s backoff (its
  children died via PDEATHSIG; slots re-request forks). Fork-server startup
  failure (bind/spawn/handshake) falls back to **stdio mode** — also forced
  by `--no-fork-server`: spawn per child, one in flight over stdin/stdout,
  SIGKILL + full respawn on hard timeout or death (the pre-fork-server
  behavior, preserved verbatim). The parent and children carry
  `PR_SET_PDEATHSIG=SIGKILL` (children re-arm it after fork since fork clears
  it) so a SIGKILLed worker cannot leak them; remaining executor pids are
  killed on exit paths (skipping any tracked pid <= 1, which would otherwise
  signal this process's own group) and the listener socket file is removed.
  **Test hook:** env `CAULI_EXEC_CMD` (whitespace-split argv) replaces the child
  command verbatim (fork-server flags are appended to the override argv),
  used by e2e to run `tests/fixtures/fake_exec.py`, which implements both
  modes. Compiled in only under `cfg(test)` / the `test-hooks` cargo feature
  -- a normal `cargo build --release` has no code path that reads this env
  var.

## Admission / backpressure

Global io semaphore `--io-concurrency` gates io execution. Cpu backlog is a
bounded channel of `2 * cpu_workers * cpu_child_threads` (twice the pool's
in-flight capacity); a dispatch that finds it full parks on
`send().await` and raises an overflow flag. The fetch loop only issues
XREADGROUP when io permits exist AND overflow == 0. Io fetch can therefore
pause while a cpu flood drains, but never indefinitely: cpu children always
make progress (hard-timeout SIGKILL bounds every slot), so overflow clears in
bounded time. This satisfies "bound cpu backlog to 2*cpu_workers" without
letting it wedge io fetching forever.

## Completion writes (all pipelined, §4.1/§4.2)

- success: `SET result EX ttl` (if store_result) + XACK + XDEL
- retry: retries+=1, `ZADD delayed (now+d)` (unknown envelope fields preserved)
  + XACK + XDEL, d per §4.2 (jitter = uniform(0.5d, d) after the max clamp);
  `cauli.Retry.countdown` overrides d
- final failure: `XADD dlq (e, reason="max_retries", error)` + result + XACK+XDEL
- malformed / unregistered / redelivery_limit: DLQ (error field empty) + XACK+XDEL

Counters: ok counts successes and duplicates; failed counts final failures;
retried counts scheduled retries; dlq counts every DLQ write.

## Known limitations / deviations (flagged)

1. **Sync hard timeout cannot kill the thread** (§4.6, documented in protocol):
   the task is failed on the retry path and the thread result abandoned; the
   ORIGINAL OS thread stays occupied until the Python call returns (which may
   be never, e.g. a blocking call with no timeout, or a C extension ignoring
   the soft-timeout injection) -- but the pool's CAPACITY does not shrink: a
   replacement thread is spawned immediately, and if the original thread ever
   does return, it just resumes serving as extra headroom. A job still
   sitting in the queue when its own dispatcher already gave up is skipped
   rather than run late.
2. **No result key** is written for `malformed` / `unregistered` /
   `redelivery_limit` DLQ entries: §4 / §4.4 specify only the DLQ write, so a
   client `get()` on such a task waits until timeout. Spec followed literally.
3. **Non-JSON `-m cauli._exec` responses** are treated as retryable
   WorkerShimError failures rather than crashing the pool.
4. Usage errors print via clap but exit 1 (spec: 1 = fatal config error;
   clap's default 2 is overridden). `--help`/`--version` exit 0.
5. Second signal exits 130 immediately; cpu children die with the worker via
   PDEATHSIG rather than explicit reaping on that path.

## Ops quickstart

```
cauli-worker --app myproj.tasks:app --queues default,emails \
    --redis-url redis://127.0.0.1:6379/0 --cpu-workers 4 --log-level info
```

Redis URL precedence: `--redis-url` > `CAULI_REDIS_URL` > `app.redis_url` (logged with
userinfo redacted, e.g. `redis://***@host/0` -- never logs a plaintext password).
Stats line every `--stats-interval`s:
`stats: fetched=N ok=N failed=N retried=N dlq=N inflight_io=N inflight_cpu=N rss_mb=N
sync_live=N sync_abandoned=N pending_async=N`.
SIGTERM/SIGINT: stop fetching, drain up to `--drain-timeout`, exit 0; second
signal exits 130. SIGKILL is safe: pending entries are re-claimed by another
worker after `--visibility-timeout` (§4.4) -- but only once idle beyond
`max(visibility_timeout, task timeout_ms + grace)`, so a legitimately
still-running long task is not reclaimed out from under itself.
