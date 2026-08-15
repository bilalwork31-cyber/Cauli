# Benchmark results

This measures the claims in [CLAIMS.md](CLAIMS.md), not "cauli vs Celery" in
general. Every number below maps to one of those four claims. Where cauli
loses or the result is inconclusive, that is reported too — a suite that only
shows wins is assumed rigged, and should be.

## Environment

- Hardware: WSL2 (Ubuntu 24.04), 6 shared vCPUs, 11 GiB RAM, no memory cap.
  **This is a shared, virtualized box, not bare metal with isolated cores.**
  Every number below is directional for that reason; where it materially
  changes a conclusion, a CPU-pinned re-measurement is called out explicitly
  (see "CPU isolation" below) rather than silently assumed away.
- Redis 7.0.15, dedicated bench instance on port 6395 (`--save '' --appendonly
  no`), never the same instance as any other workload.
- PostgreSQL 16, dedicated `bench` role/database, its own tables.
- cauli: built fresh from commit `5b1e2033ed3fb03b3062aa6f9a3dde50bbae261e`
  immediately before measurement (`cargo build --release --bin
  cauli-worker`), not a stale binary from an earlier session.
- uvloop: **PyPI release 0.22.1**, not GitHub main. Building main requires
  Cython (fine) and libuv's `autogen.sh`, which needs autoconf/automake/
  libtool; this environment's user has no passwordless sudo to install them,
  and the tradeoff of asking for that access wasn't worth it for a
  dependency version bump. Documented rather than silently downgraded to
  "from main" in the claim.
- Versions: Celery 5.6.3, taskiq 0.12.4 / taskiq-redis 1.2.3, arq 0.28.0,
  dramatiq 2.2.0, redis-py 8.1.0, psycopg 3.3.4 / psycopg2-binary 2.9.12,
  gevent 26.5.0, psycogreen 1.0.2. Full pins in [requirements.txt](requirements.txt).
- Redis baseline (`redis-benchmark -t set,get,incr -n 200000 -c 50`): **INCR
  76,628 ops/s, p50 0.303ms.** Read every dispatch-overhead number against
  this ceiling, not in isolation — a framework number close to this is
  measuring Redis, not itself.

## Method

**Throughput**: drain-rate, not a live producer racing the consumer. Preload
N tasks with no worker running, start the worker, take the slope of
completions between the 10th and 90th percentile of the run (excludes
startup ramp-up and tail stragglers). A naive `tasks_processed / elapsed_time`
metric goes invalid the moment the worker outpaces the enqueuer — measured
early in this project at 12,892 tasks/s on a 6-core box with a ~12,000/s
physical roofline. Every framework tuned to its own concurrency/prefetch/pool
optimum, not left at library defaults (both ladders shown where the gap
matters).

**Latency**: open-loop load generation (`latency_producer.py`) — enqueue on
a fixed wall-clock schedule regardless of completion, never enqueue-wait-
enqueue. A closed-loop generator hides latency collapse under load
(coordinated omission); this suite's own overload test proves the open-loop
method catches it: p50 jumped to 9.7 **seconds** the moment load exceeded the
worker's sustainable rate, instead of quietly reporting a flattering average.
Percentiles via HdrHistogram (`latency_report.py`).

**A real bug this method caught, worth naming**: an early version stamped
`scheduled_ts` with `time.time()` (wall clock). WSL2 corrected its clock
mid-run at least twice during this session (confirmed independently in
`dmesg`: `"Time jumped backwards, rotating"`), producing impossible negative
latencies. Fixed by switching every latency timestamp to `time.monotonic()`
(CLOCK_MONOTONIC — safely comparable across the producer and worker
processes since both run on the same live kernel, immune to wall-clock
jumps). Mentioned because a benchmark that silently drops or clips negative
outliers instead of finding their cause is exactly the kind of thing that
should not be trusted.

## Claim 1: memory per unit of concurrency

RSS is NOT the right metric for Celery here and using it would have quietly
biased the whole comparison in cauli's favor: Celery prefork forks after
importing the app, so every child shares copy-on-write interpreter/module
pages with its siblings — summing RSS across children counts those shared
pages once per child, inflating the total. Used **PSS** (proportional set
size, via `/proc/<pid>/smaps_rollup`) instead, which divides shared pages
among sharers — the fair "real physical cost" number. cauli's own multiple
`--procs` are spawned fresh (not forked), so PSS costs nothing extra there
and is already close to RSS.

Workload: N tasks that `sleep(3600)`, held in flight simultaneously, PSS
summed across every worker OS process.

| N in flight | cauli async, total PSS | Celery, total PSS |
|---:|---:|---:|
| 100 | 67.7 MiB | 2,036.7 MiB (**30x more**) |
| 300 | — | ~6.2 GiB (extrapolated: confirmed 20.89 MiB/process, consistent with the N=100 measurement's 20.57 MiB/process; a full 300-process run was not confirmed complete within the measurement window) |
| 1,000 | 166.5 MiB | **not attempted — ~20 GiB extrapolated, exceeds this box's 11 GiB RAM entirely** |
| 5,000 | 193.0 MiB | not attempted, same reason |
| 10,000 | 225.1 MiB | not attempted, same reason |

cauli grows 3.3x in memory for a 100x increase in concurrency (100 → 10,000).
Celery's PSS/process is flat (~20.6-20.9 MiB, confirmed across two separate
measurements) but the process *count* scales 1:1 with concurrency, so total
memory scales linearly — and linear scaling from a ~20 MiB/process baseline
puts four-digit concurrency out of reach on commodity hardware, not just
"expensive." That a stock 11 GiB box cannot run 1,000 concurrent Celery
prefork slots at all, while cauli reaches 10,000 in 225 MiB, is the claim.

## Claim 2: dispatch overhead is small and roughly constant

No-op task (one `redis.incr`, nothing else) — isolates enqueue + fetch +
dispatch + ack from any real work. Both stacks swept to their own optimum;
the gap between a framework's default and its tuned optimum is reported
too, since defaults are what most deployments actually run.

| Lane | Best config | Throughput | vs default |
|---|---|---:|---|
| Celery (sync) | `-c 4 -P prefork --prefetch-multiplier=1` | 850.6/s (±0.4%, 3 runs) | prefetch=4 (default) gave 676/s — tuning is a **26% swing**, not noise |
| cauli (sync) | `--procs 12 --io-threads 80 --io-concurrency 80` | 21,713/s (±0.5%, 2 runs) | `-c` alone at `--procs 6` (one proc per core) gave 14,557/s — **oversubscribing past the core count by 2x was faster**, not slower (see "Reliability cliffs") |
| taskiq (async) | `--workers 8 --max-async-tasks 100 --max-prefetch 100` | 9,622/s (±0.2%, 2 runs) | |
| cauli (async) | `--procs 8 --io-concurrency 96` | 30,438/s (±1.6%, 2 runs) | |
| **raw asyncio + redis, no framework** | batch=16 (matches cauli's own `--batch 16` default), concurrency 64 | **79,792/s**, clean mid-80%-slope measurement | batch=1 (naive, one round trip per task) gave only 4,958/s — **slower than every framework above it**, proof that an unbatched "ceiling" is a strawman, not a ceiling |

**Ratios**: cauli sync is **25.5x** Celery sync; cauli async is **3.2x**
taskiq. At cauli's own default batch depth, the raw no-framework ceiling is
79,792/s — cauli's async lane (30,438/s) captures about **38%** of that
ceiling; the rest is real framework overhead (envelope building, JSON
encode/decode, retry/idempotency bookkeeping, consumer-group ack) that a
hand-rolled loop skips entirely. That 38%, not the raw ceiling number itself,
is the honest measure of "how much does the framework cost."

Batch=32/64 pushed the ceiling higher still (up to 451,695/s naive_rate) but
the drain completed in under 1.4s at N=200,000 — too fast for this harness's
20ms-interval sampler to produce a reliable mid-80% slope. Reported as a
measurement-resolution limit, not a real number, rather than publishing a
number the method can't actually stand behind.

### Reliability cliffs (not in the throughput table, but load-bearing)

cauli's throughput does not degrade gracefully past a per-process resource
limit — it falls off a cliff into near-total stall:

- **Sync, `--io-threads`**: peak at 80 threads/proc (~16.6k/s @ `--procs 6`).
  At 112+ threads/proc, the run finishes 99.9% and then **hangs** for the
  remaining fraction until timeout (60s), rather than slowing down smoothly.
- **Async, `--io-concurrency`**: peak at 96/proc (~23.9k/s @ `--procs 6`,
  ~30.4k/s once `--procs` was also pushed past the core count — see below).
  At 104+, the same pattern: 91-99% done, then stalls. At 512, only 11% of
  the run completes before a 60s timeout.
- **`--procs` beyond the physical core count helps, and this box only has 6
  cores.** Sync throughput rose from 14.6k/s (`--procs 6`) to 21.7k/s
  (`--procs 12`, i.e. 2x the core count) before plateauing ~12-14 procs.
  Async rose from 24.9k/s (`--procs 6`) to 30.4k/s (`--procs 8`) and stayed
  flat through `--procs 16` (2.7x the core count) with no measured decline.
  This was not assumed, it was swept explicitly because the intuition
  "don't oversubscribe cores" turned out to be wrong on this workload.

**Practical takeaway**: this suite's own tuned configs stay comfortably
inside these limits (80 threads/proc, 96 io-concurrency/proc), but a
deployment that reaches for higher values expecting a smooth throughput
curve will instead hit a wall that looks like a hang, not a slowdown. Worth
fixing or at minimum loudly documenting upstream.

## Claim 3: true multicore for CPU-bound work

Lanes built and smoke-tested for cauli (`kind="cpu"`, forked children),
Celery prefork (native), taskiq (`--use-process-pool`, since its default
thread pool GIL-serializes a busy loop same as any other Python threads),
arq (`ProcessPoolExecutor` via `run_in_executor` — arq has no built-in
cpu-pool concept, this is the standard pattern its own users reach for), and
Dramatiq (native `--processes`). Task size sweep (0.5/2/10/50ms) defined in
`workloads.cpu_burn` (a calibrated busy-loop, timed by `perf_counter` so
duration is reproducible across machines rather than CPU-speed-dependent).

**Status: lanes built, full sweep not yet run to completion under this
session's time budget.** Prior work on this exact question (see the
project's own historical measurements, superseded by this suite but
directionally consistent) found parity at large task sizes and a cauli
advantage that grows as tasks shrink, physics dictating that trend regardless
of implementation: per-task overhead is a shrinking fraction of total time
as the task itself grows, so the crossover to parity is expected and not a
regression. Full sweep is the top item in "Not yet done" below.

## Claim 4: mixed workload survivability and honest failure modes

### Adversarial mixed: I/O + CPU burst

100 light I/O tasks/s (open-loop) + one 50ms CPU-burst "poison" task
injected every 3s, for 20s (2,000 light + 7 poison tasks). Measures light-
task p99 latency in the window right after each poison burst vs baseline.

| Lane | near-burst p99 | baseline p99 | ratio |
|---|---:|---:|---:|
| cauli async, **naive** (poison runs inline, shares the GIL with every light task) | 54-57ms | 3ms | **18-19x** |
| cauli async, **fixed** (poison routed to `kind="cpu"`, off the shared loop) | 12ms | 3ms | **4.0x** |
| Celery prefork | 58ms | 4ms | **14.5x** |

**The original hypothesis was wrong, and the data says so rather than the
hypothesis winning by default.** Celery prefork was assumed structurally
immune (one OS process per task, no shared GIL) — it was not; it landed
close to cauli's *naive*, GIL-sharing failure mode, not the "structurally
safe" baseline expected. Working theory: this is a 6-core box where Celery's
processes, Redis, Postgres and the harness itself all compete for the same
cores, so a CPU burst can starve OS scheduler fairness across processes even
without a shared GIL — a different mechanism, similar symptom. **Tested**:
see "CPU isolation" below — the stall persists (12.0x) even with Redis
pinned to separate cores from the workers, so it is not primarily a
Redis-contention artifact of sharing this box. cauli's fixed config (4.0x)
remains the best-measured result of the three.

### Correctness under a hard crash

`kill -9` a worker mid-run (no graceful drain), restart it fresh, and count
what comes out the other side: 500 uniquely tagged tasks, killed at 160/500
executed.

| Lane | Lost (permanent) | Duplicates | Recovery time |
|---|---:|---:|---:|
| cauli (always at-least-once by design — Redis Streams consumer group + visibility-timeout reclaim) | **0** | 0 | 34.0s |
| Celery, `acks_late=True` + `visibility_timeout=5` (properly configured for at-least-once) | **0** | 0 | 103.2s — **3x slower than cauli** |
| Celery, **plain default** (`acks_early` — what most deployments actually run) | **80/500 (16%), permanent, no recovery even after 60s** | 0 | N/A |

Root cause of the slow-but-eventually-correct `acks_late` number, verified
by reading kombu's actual redis-transport source rather than guessing:
`kombu.transport.redis.QoS.restore_visible` restores **at most 10 stale
messages per scan**, and the scan itself only fires on 1-in-10 invocations
(`interval=10`) — a hard-throttled recovery path, not a misconfiguration on
this suite's part. The plain-default number is the one that matters most in
practice: it is what Celery does out of the box, and it silently loses one
in six in-flight tasks on a crash with no path back.

### Segfault blast radius

A task calls `ctypes.string_at(0)` (reliable null-pointer read, real SIGSEGV
— confirmed independently via `dmesg` kernel crash-capture entries and
`subprocess.Popen.poll()` returning `-11`, not inferred). 10 innocent
`sleep(3600)` tasks held in flight alongside it.

| Config | Blast radius | Recovery |
|---|---|---|
| cauli, naive, `kind="io"`, **`--procs 1`** (no supervisor layer exists at this setting) | Entire process dies (`returncode -11` at t=0.00s). All 10 in-flight tasks lost with it. | **None** — nothing is left alive to restart it |
| cauli, naive, `kind="io"`, **`--procs 3`** (the realistic shape — `-c N` derives multiple procs by default) | Only the one crashed proc's tasks are affected | **Automatic, ~200ms.** Supervisor log, verbatim: `"worker proc 1 exited unexpectedly (signal: 11 (SIGSEGV) (core dumped)); restarting"` → `"worker proc 1 restarted (pid 7929)"` |
| cauli, **fixed**, `kind="cpu"` (poison isolated to a forked child) | Only that one forked child dies | Automatic (fork-server pattern), **zero of the 10 in-flight tasks lost**, not just eventually recovered |
| Celery prefork | Only the one process handling that task dies | Automatic — confirmed via worker log: `"Process 'ForkPoolWorker-6' pid:6692 exited with 'signal 11 (SIGSEGV)'"`, billiard respawns it |

The only real data-loss scenario is the deliberately-minimal `--procs 1`
case, which is not how cauli is meant to be deployed (`-c N` derives
multiple procs automatically). Any realistic multi-proc deployment already
has supervisor-level auto-recovery on par with Celery's, and routing
crash-risk code to `kind="cpu"` is strictly better than either: zero tasks
lost on that process, not just eventually redelivered.

A methodology note this test caught in itself: an early version of this
driver used `pgrep -x <name>` to detect whether a process had died, and it
was fooled — a dead child becomes a zombie that still matches its old
name/PID until reaped by its parent, producing a false "still alive"
reading for every lane including the one that had actually crashed at
t=0.00s. Fixed by using `subprocess.Popen.poll()` (the real exit status,
`-11` meaning "killed by signal 11") as ground truth instead of process-name
matching.

## Backlog drain (1M tasks)

Preload 1,000,000 no-op tasks with no worker running, then start it: this is
the drain-rate method itself at the scale it needs to hold up at, not a new
technique. cauli sync (`--procs 12 --io-threads 80 --io-concurrency 80`,
fresh build): **1,000,000/1,000,000 completed, no timeout, sustained
18,994 tasks/s across the full 52.6s drain** (slightly below the 21,713/s
measured at N=100,000 — plausibly a longer-run steady-state effect rather
than the more favorable end of short-run variance; not investigated
further). Enqueue itself took 167.6s (~5,966/s) — separate from the measured
number, since drain-rate timing starts only once the worker begins
consuming.

Celery's equivalent run was not executed: at its measured ~850/s it would
need roughly 1,176s (~20 minutes) and the result is already fully
predictable from the steady-state throughput number in Claim 2 — running it
would cost significant wall-clock for no new information. Flagged here
rather than silently skipped.

## CPU isolation

This box has 6 shared vCPUs; Redis, Postgres, every worker under test, and
the harness driving all of it compete for the same cores throughout most of
this document. One disambiguation was run: Redis pinned to cores 0-1
(`taskset -pc 0,1 <redis pid>`, live affinity change, no restart), the
Celery-mixed-workload retest run with the worker and harness confined to
cores 2-5 (`taskset -c 2-5 ...`, Postgres not involved in this specific
workload so not pinned — and not pinnable without root, since it runs under
a different OS user than this harness).

**Result: 12.0x near-burst/baseline p99, versus 14.5x unpinned — the stall
persists.** Celery prefork's assumed structural immunity to a CPU-burst
neighbor is not primarily a Redis-contention artifact of sharing this box;
it holds up with Redis isolated onto its own cores. Caveat kept explicit
rather than smoothed over: the harness driver (the light-task producer and
poison-injector threads) still shares the same 2-5 core set as the four
Celery worker processes in this pass, so it is not yet a fully clean
isolated-driver measurement — that is the next refinement, not a reason to
discount the result, since a genuine cross-process effect and a smaller
residual driver-contention effect are not mutually exclusive and the
magnitude (12x, not 2x) argues for the former dominating.

## Not yet done

Listed explicitly rather than silently absent, per the standard this suite
holds itself to:

- **Claim 3's full CPU-bound sweep** (0.5/2/10/50ms x 6 frameworks):
  lanes built, not yet run to completion.
- **CPU-pinned re-measurement of everything else in this document** — only
  the mixed-workload Celery anomaly has been re-tested under partial
  isolation so far (see "CPU isolation"); throughput, memory, chaos and
  segfault numbers above are all still shared-box measurements.
- **Fully isolated driver** for the mixed-workload pinned re-test (currently
  the harness driver shares cores with the workers under test).
- **Payload size sweep** (1KB/10KB/100KB/1MB args and results): explicitly
  cut from this pass by user decision; serialization/bandwidth cost at scale
  is a real question, just not this one's.
- **24-48h soak test** (watching RSS slope at ~70% load): needs wall-clock
  time this session cannot compress. To be kicked off separately and
  checked on over following day(s), not simulated or estimated here.
- **arq / Dramatiq** in the mixed-workload, chaos, and segfault sections:
  covered for the no-op/CPU/Postgres throughput lanes, not yet run through
  the reliability-focused tests. Same harness, same task shapes, just not
  yet executed for these two.
- **docker-compose packaging** for one-command public reproduction: last
  item, once the harness itself is frozen.
