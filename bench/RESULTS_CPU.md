# CPU drain-rate results: cauli vs Celery across task sizes

Date: 2026-07-29. Machine: WSL2 Ubuntu 24.04, 6 shared cores, Python 3.12.3.
Workers, Redis and the driver all share those 6 cores. 1 GiB `MemoryMax` per
worker scope, `MemorySwapMax=0`. Harness: `bench/drain.sh` + `bench/drain_driver.py`.

## Why a new harness was needed

`runner.sh`'s `driver.py` enqueues *while* the worker consumes, then reports
`exec_tps = N / (t_done - enqueue_end)`. That is valid only while the worker is
the bottleneck. Once the worker drains faster than the client enqueues, most
tasks finish before `enqueue_end`, the window collapses, and the metric reports
a rate **above the machine's physical roofline** — measured here as **12,892
tps for 0.5 ms tasks on 6 cores**, where 6 cores / 0.5 ms caps at 12,000. At
small task sizes that number was measuring the driver, not the runtime.

`drain.sh` separates the phases: fill the queue with **no worker running**,
then start the worker and sample completion count over time. Reported
`drain_tps_steady` is the slope over the middle 80% of completions, so worker
startup, child warm-up ramp, and the drying-queue tail are all excluded.
`drain_tps_wall` (total/elapsed) is reported alongside as the pessimistic
reading; both belong in the record.

## Both arms are tuned

Celery's `worker_prefetch_multiplier` is the direct analogue of cauli's
`--cpu-prefetch`: messages reserved beyond the one executing. `tasks_celery.py`
pins it to 1 for semantic fairness, which is equivalent to `--cpu-prefetch 0`.
Comparing a prefetch-tuned cauli against a prefetch-1 Celery would measure the
config, not the runtime, so **both sides were swept** and each is reported at
its own optimum.

Celery prefetch sweep at 0.5 ms (n=40000): mult 1 = 643.4, **mult 4 = 672.9**,
mult 16 = 668.2, mult 64 = 602.2. Prefetch buys Celery ~4.6% and then hurts —
its bottleneck is billiard's per-task overhead, not IPC idle time.

## Results (steady-state drain tps, 6 workers / 6 children)

| task size | Celery best | cauli best | ratio |
|---|---|---|---|
| **51 ms** (n=2000) | 67.8 (mult 1) | 72.6 (prefetch 0) | **1.07x — parity** |
| **2 ms** (n=20000) | 630.1 (mult 4) | 1733.0 (prefetch 16) | **2.75x** |
| **0.5 ms** (n=40000) | 672.9 (mult 4) | 6057.9 (prefetch 64) | **9.00x** |

Peak RSS, same runs: cauli ~61 MiB vs Celery ~178 MiB (**2.9x less**).

### The 51 ms result is physics, not a shortfall

At 51 ms per task the runtime's per-task overhead is ~2% of the work, so both
stacks are simply core-bound: 6 cores of PBKDF2 is 6 cores of PBKDF2 whoever
schedules it. Parity is the *ceiling* there. Measured spread across cauli
prefetch depths at this size (67.7–72.6) is as wide as the gap to Celery, i.e.
the difference is at the noise floor. **No amount of engineering wins this
regime**, and any claim that it does should be distrusted.

The advantage appears exactly where per-task overhead stops being noise, which
is why the sweep — not a single size — is the honest presentation.

### `--cpu-prefetch` sweep (steady drain tps)

| depth | 51 ms | 2 ms (n=8000) | 0.5 ms (n=40000) |
|---|---|---|---|
| 0 | 72.6 | 1535.4 | 1470.9 (n=15000) |
| 1 | 69.0 | 1670.5 | 2033.2 (n=15000) |
| 3 | 71.6 | 1738.2 | 2699.1 (n=15000) |
| 8 | 67.7 | 1633.9 | 3847.2 |
| 16 | — | 1733.0 (n=20000) | 4233.7 |
| 32 | — | — | 5951.0 |
| 64 | — | — | 6057.9 |

Default is 4: a large win for small tasks, noise-level at 51 ms. Depth is not
free — a child death fails everything staged behind it as retryable
`WorkerLost`, and a staged task waits out the tasks ahead of it, so deep
prefetch trades tail latency and redelivery volume for throughput.

## IO, same suite, same machine state (`runner.sh S1`, 10k tasks, 1 GiB cap)

| arm | exec_tps | peak MiB |
|---|---|---|
| celery prefork 8 | 158.4 | 220.2 |
| celery prefork 16 | 361.1 | 407.4 |
| celery gevent 500 | 86.5 | 56.2 |
| cauli sync io 500 | 514.8 | 65.2 |
| **cauli async io 500** | **18177.9** | 82.2 |

cauli async is 50.3x Celery's best arm at 5x less RAM; the sync lane is 1.43x
at 6.2x less RAM. Note gevent performed poorly under these equal-semantics
settings (acks_late, prefetch 1), consistent with the note in `RESULTS.md`.

## Caveats (read before quoting any of this)

- **Single run per cell**, no error bars. Repeat runs of one identical config
  differed by ~3% (65.0 vs 67.0 tps at 51 ms), so treat anything under ~5% as
  noise. The 2.75x and 9.00x results are far outside that; the 1.07x is not.
- All components share 6 cores; this is a contended box, not isolated hardware.
- `exec_tps` in the IO table carries the same driver-artifact caveat described
  above and is not directly comparable to the drain numbers.
- Task body is PBKDF2 (pure C, releases nothing back to Python). Results do not
  generalize to CPU tasks that spend their time in interpreted bytecode.
- Celery arms use prefork. gevent/eventlet is a different trade and is only
  represented in the IO table.
