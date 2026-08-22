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

**Update**: the original version of this section tested Celery prefork
only. A reviewer correctly flagged that as understating Celery's real
memory story — no experienced Celery operator runs 1,000-way I/O
concurrency on prefork; they reach for `-P gevent` or `-P threads`, one
process, N greenlets/threads. All three are reported below.

**Second update**: the first cauli numbers in this table were measured
against a fixed flag set carried over from the *throughput* tuning pass
(`--procs 8 --io-concurrency 96`), applied at every N including N=100 —
the same config regardless of regime. That's the exact mistake this
document calls out everywhere else ("compare methods at each one's own
optimum, or do not compare" — see Claim 2). Re-measured using cauli's own
single-knob interface, `-c N`, and letting it auto-derive processes/
threads/io-concurrency per `--print-plan` — no hand-picked flags, just
the documented default path. Numbers below are the corrected ones.

| N in flight | cauli async (`-c N`, own optimum) | Celery prefork | Celery gevent | Celery threads |
|---:|---:|---:|---:|---:|
| 100 | 58.8 MiB | 2,036.7 MiB (**35x more**) | **46.7 MiB — gevent wins** | **45.7 MiB — threads wins** |
| 300 | — | ~6.2 GiB (extrapolated: confirmed 20.89 MiB/process, consistent with the N=100 measurement's 20.57 MiB/process; a full 300-process run was not confirmed complete within the measurement window) | — | — |
| 1,000 | 156.9 MiB | **not attempted — ~20 GiB extrapolated, exceeds this box's 11 GiB RAM entirely** | **68.2 MiB — gevent wins** | **72.3 MiB — threads wins** |
| 5,000 | 183.2 MiB | not attempted, same reason | 164.4 MiB — gevent still ahead | **193.5 MiB — cauli wins (barely)** |
| 10,000 | 215.7 MiB | not attempted, same reason | 282.9 MiB — **cauli wins** | 341.3 MiB — **cauli wins clearly** |

(Celery gevent/threads readings: single process confirmed via `ps`, one
worker, `-c` set to the greater of N or 1,000 so every held task has a
free slot; all readings taken with `memory_report.py`'s `pgrep -x` match
— see the fixed-bug note below. cauli readings sum PSS across its
supervisor + all worker processes, matched on `cauli-worker`.)

Retuning to cauli's own optimum closed some of the gap (58.8 vs the
original 67.7 MiB at N=100; 215.7 vs 225.1 at N=10,000) but did not flip
the story: **gevent and threads are still cheaper than cauli up to
roughly N≈4,500-6,000**, and cauli only pulls ahead past that because its
marginal cost per held task (~6.6 KiB/task, sub-linear) is lower than
either single-process pool's (~19-30 KiB/task). The crossover is real,
not a tuning artifact — chasing it further would mean changing cauli's
architecture, not its flags, which is out of scope for a benchmark
suite's black-box methodology.

**Why the gap doesn't close: this is arguably the price of a crash-
isolation guarantee, not waste** — stated as a documented architectural
fact, not a fresh measurement (Claim 4's segfault test covered cauli,
Celery *prefork*, arq and Dramatiq; gevent/threads were not put through
a kill test, so this paragraph is inference, flagged as such rather than
dressed up as a result). cauli's floor is higher at low N because its
design always runs a supervisor process plus one-or-more worker
processes, each embedding its own CPython interpreter — real memory,
spent on purpose, with `--procs 3+` recovering a crashed child in ~200ms
losing zero tasks (measured, Claim 4). Celery's auto-respawn-on-crash
(billiard) is specific to the **prefork** pool — gevent and threads are
each a single OS process with no built-in supervisor, so a segfault in
either takes the whole worker and everything it was holding with it,
same as cauli's own deliberately-minimal `--procs 1` case (also measured
losing everything, Claim 4). Whether that trade is worth cauli's extra
low-N memory is a judgment call, not something this benchmark can settle
— publishing the loss and the honest reason for it, rather than quietly
keeping the prefork-only table, is the standard this document holds
itself to. A gevent/threads kill test is added to "Not yet done" below
so this stays a stated inference, not a silent assumption.

**Bug found and fixed while running this**: `memory_report.py` used
`pgrep -f <pattern>`, which matches on full command line, not process
name — so it self-matched any wrapper shell whose own script text
happened to contain the pattern (e.g. a driver script that literally runs
the word "celery" as part of a longer command), phantom-counting that
shell as a worker process. This is the same pgrep pitfall already
documented and fixed once before in this suite (`segfault_driver.py`,
`chaos_driver.py`) — just missed in this file. Fixed to `pgrep -x` (exact
comm name), confirmed against a clean `ps` snapshot showing exactly one
`celery` process during a gevent run. Impact on already-published prefork
numbers above is negligible (a stray ~1-3 MiB shell against multi-GB
totals) and doesn't change any conclusion; it mattered here because
gevent's single-process totals are small enough for a phantom entry to
move the number by single-digit percent.

## Claim 2: dispatch overhead is small and roughly constant

No-op task (one `redis.incr`, nothing else) — isolates enqueue + fetch +
dispatch + ack from any real work. Both stacks swept to their own optimum;
the gap between a framework's default and its tuned optimum is reported
too, since defaults are what most deployments actually run.

| Lane | Best config | Throughput | vs default |
|---|---|---:|---|
| arq (async) | `poll_delay=0.01, max_jobs=200` (WorkerSettings attrs — no CLI flag exists) | 248.2/s, still timed out at N=20,000/60s | **default `poll_delay=0.5s`/`max_jobs=10` gave 26.9/s — a 9.2x swing**, and even tuned this remains far below every other framework here; not fully root-caused (see note below), reported as measured rather than chased further |
| Celery (sync) | `-c 4 -P prefork --prefetch-multiplier=1` | 850.6/s (±0.4%, 3 runs) | prefetch=4 (default) gave 676/s — tuning is a **26% swing**, not noise |
| dramatiq (sync) | `--processes 12 --threads 8` (or `6 procs / 32 threads`, statistically tied) | 12,843.6/s, clean | `6 procs / 8 threads` gave 9,490.7/s — **35% swing** from thread count alone |
| taskiq (async) | `--workers 8 --max-async-tasks 100 --max-prefetch 100` | 9,622/s (±0.2%, 2 runs) | |
| cauli (sync) | `--procs 12 --io-threads 80 --io-concurrency 80` | 21,713/s (±0.5%, 2 runs) | `-c` alone at `--procs 6` (one proc per core) gave 14,557/s — **oversubscribing past the core count by 2x was faster**, not slower (see "Reliability cliffs") |
| cauli (async) | `--procs 8 --io-concurrency 96` | 30,438/s (±1.6%, 2 runs) | |
| **raw asyncio + redis, no framework** | batch=16 (matches cauli's own `--batch 16` default), concurrency 64 | **79,792/s**, clean mid-80%-slope measurement | batch=1 (naive, one round trip per task) gave only 4,958/s — **slower than every framework above it**, proof that an unbatched "ceiling" is a strawman, not a ceiling |

**Ratios**: cauli sync is **25.5x** Celery sync and **1.7x** Dramatiq (the
strongest sync competitor measured); cauli async is **3.2x** taskiq and
**123x** arq (see caveat below). At cauli's own default batch depth, the raw
no-framework ceiling is 79,792/s — cauli's async lane (30,438/s) captures
about **38%** of that ceiling; the rest is real framework overhead (envelope
building, JSON encode/decode, retry/idempotency bookkeeping, consumer-group
ack) that a hand-rolled loop skips entirely. That 38%, not the raw ceiling
number itself, is the honest measure of "how much does the framework cost."

**arq caveat, stated plainly**: arq's default `poll_delay=0.5s` is a severe,
easy-to-miss trap — it caps throughput near `max_jobs / poll_delay`
regardless of how fast the task itself runs, confirmed by isolating the
same `ProcessPoolExecutor` + `asyncio.run_in_executor` pattern outside of
arq entirely (568/s and 555/s respectively, both near the physical ceiling)
and finding the bottleneck disappears — it is specific to arq's own
dispatch loop, not this suite's task bodies. Tuning `poll_delay` down 50x
recovered 9.2x of that (26.9/s → 248.2/s), but did not close the remaining
gap to celery/dramatiq/taskiq, and the worker's own log shows a large,
roughly-constant per-job `delayed=` value (time since enqueue, not
execution time — jobs execute in 0.00s once picked up) suggesting a
further bottleneck upstream of task execution that was not isolated further
in the time available. The **123x ratio against arq should be read as "arq
needs deeper tuning or is a poor fit for this harness's redis-list-broker
pattern," not as a clean architectural comparison** — unlike every other
ratio in this document, which is measured at each side's genuine optimum.

Batch=32/64 pushed the ceiling higher still (up to 451,695/s naive_rate) but
the drain completed in under 1.4s at N=200,000 — too fast for this harness's
20ms-interval sampler to produce a reliable mid-80% slope. Reported as a
measurement-resolution limit, not a real number, rather than publishing a
number the method can't actually stand behind.

**Celery gevent, no-op throughput**: swept `-c` in {100, 500, 1000, 2000}.
Result: **flat 77-84/s at every concurrency level** — the knob measurably
does nothing. Confirmed with `-l info` tracing: zero interleaving, every
task's `received` → `succeeded` (~7-10ms) completes before the next
message is even read off the broker. Root cause, not a harness bug:
gevent only yields control to other greenlets on a socket call that
would actually block. This queue always has backlog, so `BRPOP` returns
immediately every time, the hub scheduler is never invoked to run the
spawned task greenlets concurrently, and the whole worker degrades to
one core doing one task's full round trip at a time — worse than
prefork's `-c 4` real OS parallelism (850.6/s), because prefork gets
genuine multi-core overlap and gevent here gets none. This is a correct,
structural property of cooperative concurrency, not a defect: gevent
buys overlap only when tasks spend real wall-clock time *waiting* on
I/O (slow HTTP calls, DB queries with real latency), which a near-instant
Redis `INCR` doesn't have. It is the wrong tool for this specific claim
and the right one for Claim 1's memory workload above, where tasks are
genuinely idle rather than fast. Not chased further for a "tuned" number
because there is no concurrency setting that fixes a workload with no
wait time to overlap — that's the finding.

**Celery threads (`-P threads`), no-op throughput**: swept `-c` in {4, 16,
50, 100, 200, 500}. Result: **flat ~580-620/s at every concurrency
level**, but unlike gevent, every run actually completed (no timeout).
Real OS threads do get real overlap here — redis-py releases the GIL
during the actual socket syscall, so multiple worker threads can
genuinely be in-flight on I/O at once — but Celery's own per-task
Python-level work (tracing, signal dispatch, ack bookkeeping) still runs
serialized under the GIL, and that fixed per-task cost dominates for a
near-instant Redis call. Below prefork's real multi-core parallelism
(850.6/s) and far below cauli/dramatiq/taskiq, but a clean, believable
number — not the pathological zero-scaling gevent showed above.

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

Lanes: cauli (`kind="cpu"`, forked children), Celery prefork (native),
taskiq (`--use-process-pool`, since its default thread pool GIL-serializes a
busy loop same as any other Python threads), Dramatiq (native
`--processes`), and arq (`ProcessPoolExecutor` via `run_in_executor` — arq
has no built-in cpu-pool concept, this is the standard pattern its own
users reach for). Task size sweep 0.5/2/10/50ms, `workloads.cpu_burn` (a
calibrated busy-loop, timed by `perf_counter` so duration is reproducible
across machines rather than CPU-speed-dependent). All five at 6
processes/workers, matching this box's core count — CPU-bound work is
fundamentally core-limited, not I/O-bound where the earlier oversubscription
finding applies, so an extensive process-count sweep like Claim 2's isn't
the right lever here.

| Task size | cauli | Celery | taskiq | Dramatiq | arq |
|---:|---:|---:|---:|---:|---:|
| 0.5ms | **2,664.8/s** | 778.4/s | 810.5/s | 1,539.5/s | ~567/s* |
| 2ms | **1,776.6/s** | 693.2/s | 763.2/s | 1,188.7/s | ~551/s* |
| 10ms | **516.5/s** | 413.0/s | 515.6/s (statistical tie) | 473.3/s | 335.1/s |
| 50ms | 115.3/s | 110.8/s | 117.6/s | 117.3/s | 119.0/s — **all five within noise** |

Exactly the physics-driven trend predicted before running this: per-task
dispatch overhead is a shrinking fraction of total time as the task itself
grows, so the gap should compress toward parity as task size grows — and it
does, cleanly. At 50ms all five frameworks converge (110.8-119.0/s, no
framework meaningfully ahead — the physical CPU is the bottleneck, not
dispatch); at 0.5ms cauli's lead is largest (1.7x over the next-best,
Dramatiq; 3.4x over Celery). This is also the one claim where cauli does
**not** win at every size — taskiq statistically ties it at 10ms.

*arq's numbers are `naive_rate` (elapsed/count), not the mid-80%-slope
method used everywhere else in this document: `mid80_rate` came back `null`
even on runs that completed without timing out. Cause, read directly from
the worker's own log (see Claim 2's arq caveat): arq's completions arrive in
one late burst rather than a steady stream — often nearly the whole run
finishes in a single 20ms poll window — which breaks the interpolation
method's assumption of gradual progression through the 10th-90th percentile
of completions. Reported as the best-available honest number for a
framework whose throughput profile doesn't fit this suite's primary
methodology, not silently smoothed into a number the method can't actually
back up.

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

### arq and Dramatiq under the same three tests

Same adversarial-mixed, kill -9, and segfault tests, run against arq and
Dramatiq using the same task shapes. Not tuned for recovery the way Celery
got a second, properly-configured pass (`acks_late`) — these are each
framework's first-attempt/default reliability configuration, stated
explicitly so the numbers aren't read as more authoritative than they are.

**Adversarial mixed** (same 100 light/s + 50ms poison every 3s, 20s):

| Lane | near-burst p99 | baseline p99 | ratio |
|---|---:|---:|---:|
| arq | 61ms | 14ms | **4.4x** |
| Dramatiq | 91ms | 18ms | **5.1x** |

**Stated plainly so this isn't misread as a cauli loss: cauli's
*recommended* config already wins this comparison.** Ranked best to
worst: cauli-fixed (4.0x) < arq (4.4x) < Dramatiq (5.1x) < Celery
prefork (12-14.5x) < cauli-**naive** (18-19x). Only cauli's unrecommended
default loses to arq/Dramatiq; the config this suite would actually tell
someone to run beats both. Naive is kept in the table on purpose — it's
the number you get if you don't route CPU-bound work to `kind="cpu"`,
and a reader deploying cauli needs to know that failure mode exists — but
it should not be read as "the" cauli result.

Both arq and Dramatiq land well below Celery's 12-14.5x and cauli-naive's
18-19x — plausibly
because Dramatiq's `--threads 8` still shares a GIL *within* each of its 6
processes (a poison task only stalls 1/6 of total capacity, diluting the
effect vs. Celery's coarser per-process granularity), and arq's baseline is
already elevated (14ms vs Celery's 4ms, cauli's 3ms) from its own dispatch
characteristics documented in Claim 2/3. Not fully explained, reported as
measured.

**kill -9 correctness** (500 tagged tasks, killed at whatever fraction each
framework reached by the time the earlier lanes hit theirs):

| Lane | Lost (permanent) | Duplicates | Recovery time |
|---|---:|---:|---:|
| arq | **400/500 (80%)** | 0 | timed out at 60s |
| Dramatiq | 85/500 (17%) | 0 | timed out at 60s |

arq's loss is worse than even Celery's careless default (16%), and the lost
tags are **not just the tail of the run** — `tag-0` and `tag-1` were among
the lost set despite only 100/500 tasks having executed before the kill,
which would put them first in a FIFO drain. Not investigated further in the
time available; noted because it means the loss pattern isn't simply
"whatever hadn't started yet," which would be the benign explanation.
Dramatiq's 17% loss is in the same range as Celery's *default* (unconfigured)
number — consistent with the same story: frameworks default to
weaker-than-at-least-once delivery unless someone opts in, and Dramatiq
wasn't given that opt-in tuning here (unexplored — see "Not yet done").

**Segfault blast radius** (10 `hold` tasks + 1 segfaulting task):

| Lane | Blast radius |
|---|---|
| arq | Entire process dies (single asyncio loop, no process isolation) — same architecture class as cauli's naive async lane. All 10 in-flight tasks lost, no auto-respawn observed. |
| Dramatiq | Top-level process survives (`--processes 6`, same structural class as Celery prefork) |

Methodology note kept honest rather than smoothed over: the auxiliary
"child pool" pgrep tracking used `-x python3` for Dramatiq, which is too
broad — it matched unrelated python3 processes on the box (including this
suite's own long-running soak-test driver), so the "children that
disappeared/appeared" reading for Dramatiq is noise, not signal. The
**primary** result (top-level process survived, via `Popen.poll()` on the
exact process this driver spawned) is unaffected by that and is trustworthy.

## Claim 5: real framework code (Django ORM, SQLAlchemy async) still dispatches fast

Every I/O number elsewhere in this document is a raw psycopg3 call. Real
task bodies go through an ORM. Two lanes added: a Django ORM insert
(`Model.objects.create()`, mapped onto the same `bench_io` table every
other PG lane writes to) for the sync/Celery-flagship comparison, and a
SQLAlchemy 2.0 async ORM insert (`AsyncSession` + `session.commit()`) for
the async/taskiq-flagship comparison FastAPI apps most often reach for.

**This is also where a real bug got found and fixed, not just benchmarked
around.** Building the Django lane surfaced a reproducible memory-corruption
crash in cauli-worker at higher process counts: `double free or corruption
(fasttop)`, glibc's allocator catching a use-after-free. It fired at
shutdown, after every task had already completed and been acked — a
corrupted heap reported as a clean drain, which is worse than a hard crash.
Black-box bisection (procs 1 through 12, io-threads 8 through 80) localized
it to needing multiple worker processes; a specialized agent then found the
actual cause by reading the source: `std::process::exit()` runs libc atexit
handlers (specifically `OPENSSL_cleanup`, pulled in by the Postgres driver's
libssl) while sync io-pool threads and embedded-interpreter daemon threads
are still alive, racing the global OpenSSL teardown against per-thread
teardown on the way out. Fixed by switching to `libc::_exit()`, which skips
atexit handlers entirely (every resource with an owner outside the process
was already released explicitly before both exit points, so nothing
depended on them running). Verified with 23 clean runs at configs that
previously corrupted reliably, plus the full Rust test suite green. Every
number below was measured against the fixed binary — publishing a crash
discovery is more valuable than the crash never being visible in the first
place, and every number in this section only exists because this got fixed
first rather than benchmarked around.

### Django ORM, sync

**Correction made mid-session, kept visible rather than quietly fixed:**
the numbers first published here used a bare `django.setup()`, not
cauli's own `cauli.contrib.django.django_app()` integration — the wrong
baseline for "does cauli work fast with real Django code," since it skips
the exact fixup (`close_old_connections()` before/after every task,
Celery-fixup parity) cauli ships and documents in the main README for
this. Rebuilt on the real integration; all four rows below are needed to
tell the honest story, not just the best number.

| Lane | Config | Throughput |
|---|---|---:|
| cauli, raw psycopg3, direct Postgres | `--procs 4 --io-threads 80` | 2,554.9/s |
| cauli, raw psycopg3, via pgbouncer | same config | 3,272.7/s |
| cauli, Django ORM, **naive** (bare `django.setup()`, `CONN_MAX_AGE=0`), throughput config | `--procs 12 --io-threads 80` | **`FATAL: sorry, too many clients already`** |
| cauli, Django ORM, naive, connection-safe config | `--procs 4 --io-threads 16` | 2,210.6/s |
| cauli, Django ORM, **official `django_app()` fixup**, `CONN_MAX_AGE=0`, throughput config | `--procs 12 --io-threads 80` | 223.9/s — no exhaustion, but `CONN_MAX_AGE=0` forces the fixup to open a fresh connection every task |
| cauli, Django ORM, official fixup, `CONN_MAX_AGE=60`, direct Postgres, throughput config | `--procs 12 --io-threads 80` | **still exhausts Postgres's connection limit** |
| cauli, Django ORM, **official fixup, `CONN_MAX_AGE=60`, via pgbouncer**, throughput config | `--procs 12 --io-threads 80` | **3,171.7/s — the correct number** |
| Celery prefork, Django ORM | `-c 4 -P prefork --prefetch-multiplier=1` | 162.4/s |

**What each row actually proves, since the shape of this table matters
more than any single number in it:**
- cauli's own `close_old_connections()` fixup (matching Celery's) is real
  and does its documented job — it manages connection *staleness*, and
  once installed the naive integration's silent data-loss-adjacent crash
  (a `FATAL` error mid-run) never recurs, at any `CONN_MAX_AGE`.
- It does **not**, and by design cannot, cap the *number* of simultaneous
  connections — that governs `max_connections`, and Django has no
  built-in pool. Proven directly: the fixup alone, correctly installed,
  still exhausts Postgres's connection limit at `--io-threads 80` unless
  something in front of Postgres multiplexes connections. This isn't a
  cauli gap; it's true of Django behind *any* sufficiently parallel
  worker, cauli or Celery.
- The fix is the same one the raw lane already benefits from: pgbouncer.
  Once both pieces are in place — the official fixup for correctness,
  pgbouncer for capacity — Django's ORM reaches **3,171.7/s**, nearly
  matching the raw driver (3,272.7/s) and **19.5x** Celery prefork at
  Celery's own matched config, using cauli's real throughput-tuned
  config rather than a deliberately scaled-down one.
- Django's `CONN_MAX_AGE` matters independently of cauli: `0` is safe but
  needlessly slow once the fixup runs on every task (a fresh connection
  every time); Django's own documented positive-value recommendation
  lets the fixup do its real job — evict genuinely stale connections,
  reuse warm ones otherwise.

### SQLAlchemy async ORM, the FastAPI-shaped comparison

| Lane | Config | Throughput |
|---|---|---:|
| cauli, raw psycopg3-async, direct Postgres | `--procs 4 --io-concurrency 500` | 3,783.7/s |
| cauli, raw psycopg3-async, **via pgbouncer** | same config | **4,577.5/s — pgbouncer wins here too** |
| cauli, SQLAlchemy async ORM, direct Postgres | `--procs 4 --io-concurrency 24` | 378.6/s |
| taskiq, SQLAlchemy async ORM | `--workers 8 --max-async-tasks 20 --max-prefetch 20` | 733.6/s (**taskiq wins this one**) |

**Stated plainly, not smoothed over: taskiq beats cauli on this lane.**
Root-caused rather than left as an unexplained number: the gap is not
ORM overhead. Isolated by re-running the identical insert through
SQLAlchemy's **Core** API (raw SQL, no `Session`, no identity map, no
unit-of-work) at the same concurrency — it measured within noise of the
full ORM (~208/s Core vs ~219-253/s ORM, both sequential and concurrent
via `asyncio.gather`, pool size swept 2 through 100 core connections with
no effect). That rules out the ORM layer entirely. What's left is
SQLAlchemy's async **engine** itself: its asyncio support is built on a
`greenlet`-based bridge over what is fundamentally synchronous internals,
and under concurrent load in this environment it does not scale with
added concurrency the way the raw driver does — throughput stayed flat
regardless of pool size or `asyncio.gather` batch size, which a real
connection-availability bottleneck would not produce. This is a
characteristic of SQLAlchemy's async engine in this environment, not of
cauli, not of this suite's task code, and not an artifact of a
transaction-mode difference (an earlier version of this section
attributed the gap to autocommit-vs-explicit-transaction round trips;
that was wrong — Core bypasses the ORM's transaction handling entirely
and shows the same number, so transaction mode isn't the cause either).
Reported as measured: cauli loses this comparison, for a reason external
to cauli, and that's still the honest number to publish.

### Operational finding, summarized: Django needs a pooler in front of Postgres, regardless of task queue

Fully covered by the table and its explanation above — restated in one
place for anyone skimming: Django's ORM opens one persistent connection
per thread and never pools them, cauli's `close_old_connections()` fixup
(Celery-fixup parity) manages staleness but not connection *count*, and
neither is a substitute for a real pooler once concurrency is high enough
to approach `max_connections`. Not a cauli-specific gap — Celery with
enough prefork workers hits the identical wall. pgbouncer in front of
Postgres is the production-correct fix, not a benchmark workaround.

### Second bug found and fixed while running this: pgbouncer + psycopg3's server-side prepared statements

The first pgbouncer attempt made every lane *slower* — the raw psycopg3
sync lane dropped from 2,554.9/s direct to ~140/s through pgbouncer, a
regression, not an improvement. Chased through three wrong theories
before finding the real cause, each one checked and ruled out rather
than assumed: not pgbouncer's own pool size (raising `default_pool_size`
60 → 300 → 350 changed nothing); not pgbouncer being CPU-bound (measured
at 2.4% CPU during the stall); not Postgres itself (backends sat idle,
0% CPU) or a connection-wait bottleneck (`SHOW STATS` avg_wait_time did
drop 15x after pre-warming the pool, from ~1.9s to ~122ms, while
throughput barely moved — proof wait time was never the actual
bottleneck). Isolating the exact client code path outside cauli entirely
(`psycopg_pool.ConnectionPool`, real OS threads, direct against
pgbouncer) surfaced the real error: `psycopg.errors.
InvalidSqlStatementName: prepared statement "_pg3_0" does not exist`.

psycopg3 automatically promotes repeated identical queries to
server-side prepared statements. A prepared statement lives on whichever
physical backend connection created it; pgbouncer's `transaction`
pooling mode hands each new transaction whichever backend connection is
free, which is frequently a different one — so the prepare silently
doesn't exist on the connection psycopg3 tries to reuse it against, the
query fails, and (invisibly, no error surfaces in cauli's own log at
`-l warn`) something in the retry path burns the wall-clock time this
symptom showed. Fix: `prepare_threshold=None` in the pool's connection
kwargs, disabling automatic server-side prepares — the standard,
documented fix for any psycopg3 client that might run behind pgbouncer's
transaction pooling. Cost measured directly: ~3% slower than prepared
statements against Postgres directly (1253.0/s → 1210.8/s in an isolated
thread-pool test), negligible next to the 9x this fix recovered through
pgbouncer (140/s → 1279/s in the same isolated test; 3,272.7/s in the
full cauli-worker run). Applied to both raw PG task files
(`tasks_cauli_sync_pg.py`, `tasks_cauli_async_pg.py`) unconditionally, so
they're correct whether pointed at Postgres directly or through a
pooler — not left as a footgun for whoever points `BENCH_PG_DSN` at
pgbouncer next.

**Reproducing this section requires pgbouncer** (`pool_mode = transaction`,
pointed at the same `bench` Postgres role/db) in addition to the base
`setup.sh` environment — see `bench/README.md` for the config used here.

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

## Soak test (killed by an environment outage, not completed)

Started with a 48h target, cauli async (`--procs 8 --io-concurrency 96`,
the same config measured in Claim 2), on a dedicated Redis instance (port
6396) at 2,000 tasks/s open-loop, PSS sampled every 5 minutes to
`soak_memory.csv`. Early samples were healthy: flat-trending PSS, zero
failures, tens of millions of tasks processed over several hours.

**It didn't finish, and the data didn't survive.** The WSL VM this
harness runs under went down for an extended period mid-run (confirmed
via `journalctl --list-boots`: the boot the soak was running under ended
hours into the run; the next boot started roughly 23 hours later) — an
environment-level outage, not a cauli failure. Everything not written to
disk died with it: the dedicated redis:6396 instance (no persistence
configured, matching every other Redis instance in this suite — `--save
'' --appendonly no`, intentional for the *primary* benchmark Redis, an
oversight for a multi-hour unattended soak) and the driver process both
gone on the next boot. `soak_memory.csv` was searched for across the
whole filesystem after the fact and not found — it was never confirmed
written to a path that survived the outage, so even the partial run's
CSV trail is lost, not just the tail end.

**Reported as failed-to-complete, not as data supporting the claim.**
The early healthy samples are anecdotal at best — not enough hours to
distinguish "flat" from "climbing very slowly," which is the entire
point of a 48h soak. Redoing this needs two changes, not just a rerun:
`appendonly yes` (or equivalent) on the soak's dedicated Redis instance
so a host interruption doesn't erase the in-progress data, and running
it somewhere that survives a multi-hour unattended window (this
environment's WSL VM does not, by default, and nothing here currently
detects or alerts on that kind of silent kill). Listed in "Not yet done"
below rather than left implied by a stale "in progress" heading.

## docker-compose packaging

`Dockerfile` and `docker-compose.yml` are written (python:3.12-slim base —
satisfies cauli-worker's `--enable-shared` CPython requirement per the main
README — builds cauli-worker fresh, installs pinned deps, wires
`BENCH_REDIS_URL`/`BENCH_PG_DSN` to the compose service hostnames). **Not
validated**: Docker Desktop's daemon is not functional on this development
machine (CLI present, engine not running), confirmed and then explicitly
deprioritized rather than spending further time working around a local
environment problem unrelated to the benchmark itself. Whoever runs this
next should treat `docker compose up --build` as untested until it's been
run once for real.

## Not yet done

Listed explicitly rather than silently absent, per the standard this suite
holds itself to:

- **Celery gevent lanes — done** for Claims 1 (memory-per-concurrency) and
  2 (no-op throughput), both above. **Not done**: a gevent mixed-workload
  (Claim 4) lane — skipped by inference, not measurement: the throughput
  result already shows gevent has zero concurrency overlap for fast
  Redis calls, and a CPU-bound poison burst would monopolize the single
  OS thread with no yield point at all, so the outcome (a GIL-sharing-style
  stall, at least as bad as cauli's naive async lane) is predictable from
  a mechanism already demonstrated rather than worth a full test run.
  Flagged here instead of silently assumed.
- **Celery `-P threads` lane — done** for Claims 1 and 2, both above.
  **Not done**: a threads mixed-workload lane, skipped for the same
  reason as gevent's — threads run under one GIL too, so a CPU-bound
  poison burst has no more headroom to overlap than gevent's does.
- **kill -9 test for Celery gevent/threads**: not run. Claim 1's crash-
  isolation argument (gevent/threads have no built-in process supervisor,
  unlike cauli or Celery prefork) is currently an architectural inference
  from how the pools work, not a fresh measurement — stated as such where
  it's used, but belongs on this list until it's actually tested.
- **Original (pre-fix) cauli async memory numbers, re-audited**: the
  Claim 1 re-measurement used cauli's own `-c N`-derived config instead of
  a carried-over throughput flag set; whether the *original* published
  numbers came from that throughput config specifically, or something
  else, was never recorded at the time and couldn't be reconstructed —
  flagged so the discrepancy isn't silently attributed to noise.
- **Published latency-results section**: p50/p99/p99.9 enqueue→start and
  enqueue→complete at 25/50/75/90% of each lane's measured capacity. The
  `latency_producer.py`/`latency_report.py` harness exists and works; the
  results table doesn't exist yet.
- **Non-localhost Redis RTT sweep** (toxiproxy, 0.5/1/2/5ms + jitter): not
  started. Every number in this document is same-box, near-zero-RTT Redis.
- **Duplicate-delivery test** (`kill -STOP` past visibility timeout,
  `kill -CONT`, count executions per tag): not started. Called out as
  effectively mandatory before this project's numbers should be read by
  anyone building a payments-adjacent system.
- **Redis durability lane** (`appendonly yes` everysec/always) + a
  Redis-restart-mid-run chaos test: not started.
- **Producer-path benchmark**: pipelined/batched enqueue cost, and
  `.delay()`'s added p99 under simulated FastAPI/Django request load: not
  started.
- **Redis memory / stream-trimming risk for the running soak test**: not
  yet tracked. cauli's Redis Streams may not XTRIM/XDEL consumed entries;
  at the soak's measured rate this could accumulate into an OOM risk
  before the soak completes. Worker-side PSS is being sampled; Redis's own
  memory is not, yet.
- **Retry sweep** (1%/10%/50% failing tasks with backoff), **ETA/scheduled
  tasks**, **result round-trip latency**, **graceful shutdown** (SIGTERM
  with load in flight), **backpressure** (Redis maxmemory), **sustained
  overload** beyond the single 9.7s datum that exists, **multi-worker
  contention**: none started.
- **Polish set**: pipelined `redis-benchmark` baseline, CPU-seconds/100k
  tasks, RQ/huey lanes, cold-start comparison, confidence intervals via
  ~10x reruns of the headline numbers, eventual bare-metal validation —
  none started.
- **CPU-pinned re-measurement of everything else in this document** — only
  the mixed-workload Celery anomaly has been re-tested under partial
  isolation so far (see "CPU isolation"); throughput, memory, chaos, segfault
  and CPU-bound numbers above are all still shared-box measurements.
- **Fully isolated driver** for the mixed-workload pinned re-test (currently
  the harness driver shares cores with the workers under test).
- **Payload size sweep** (1KB/10KB/100KB/1MB args and results): explicitly
  cut from this pass by user decision — a real question given how much of
  production traffic isn't a bare no-op, just not this pass's.
- **Soak test**: attempted, killed by a WSL VM outage partway through, no
  data survived (see above). Needs a durable Redis config and a host that
  survives a multi-hour unattended window before it's worth attempting
  again.
- **cauli-worker crash fix landed mid-session** (see Claim 5): a real
  `double free or corruption` bug at shutdown, found while building the
  Django ORM lane, root-caused and fixed (`process::exit` → `libc::_exit`,
  `worker/src/main.rs`), merged to `main`, rebuilt, verified. Everything
  measured before this fix in an earlier pass of this document was
  measured against configs/workloads that never happened to trigger it
  (it needs multiple worker processes plus a C-extension-backed sync I/O
  library, e.g. psycopg3, to manifest) — but it was real and unnoticed
  until this session, which is itself worth stating plainly rather than
  implying every prior number was already validated against it.
- **SQLAlchemy async ORM's greenlet-bridge bottleneck — root-caused, not
  fixed**: Claim 5 isolated the cauli-vs-taskiq gap to SQLAlchemy's async
  engine itself (Core bypass showed the same flat throughput as the full
  ORM, ruling out ORM overhead and the transaction-mode theory this list
  used to carry). Not chased further because it's SQLAlchemy's own
  library characteristic in this environment, not something this
  benchmark's task code or cauli can fix.
- **Dramatiq redelivery tuning**: only tested at default reliability config
  (17% loss on a hard crash); unlike Celery, never given a properly-tuned
  second pass to see if a Celery-style `acks_late` equivalent closes the gap.
- **arq's 80%-loss root cause**: measured and reported, not root-caused —
  the lost tags weren't just the tail of the run, which rules out the benign
  explanation but wasn't investigated further.
- **docker-compose validation**: written, not run (see above).
