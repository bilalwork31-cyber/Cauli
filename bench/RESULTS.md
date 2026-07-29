# rupy vs Celery benchmark results

Date: 2026-07-20. All runs on the same WSL2 VM (Ubuntu 24.04, 6 cores, 11GB RAM,
Python 3.12.3, Redis 7.0.15, kernel cgroup v2), one system under test at a time,
identical memory caps via `systemd-run --user --scope -p MemoryMax=... -p MemorySwapMax=0`
(swap cap is mandatory on WSL2 or the memory cap silently leaks into swap; the cap
mechanism was proven by OOM killing a memory hog at 100M, exit 137).

Workloads (identical code imported by both stacks from `common.py`):

- io task: `requests.get` against a local mock external API that sleeps 50ms per call
  (mock verified to sustain >5000 rps standalone, so it is not the bottleneck for Celery).
- io_async task (rupy only): same endpoint via a keepalive asyncio connection pool.
- cpu task: `hashlib.pbkdf2_hmac("sha256", ..., 94000)` calibrated to 51.0ms per call.

Fairness: same machine, same Redis instance (throwaway, port 6390), sequential runs,
FLUSHALL between scenarios, 200 task warmup before each measured run, both stacks store
results, Celery configured production safe like rupy (`task_acks_late=True`,
`worker_prefetch_multiplier=1`, at least once semantics on both sides). CPU uncapped for
both. Latency is end to end per task: enqueue time to completion time. `full_tps` is
N / (first enqueue to last completion); `exec_tps` is N / (enqueue finished to last
completion) and overstates fast workers that finish most work during the enqueue phase,
so `full_tps` is the headline number.

## S1: 10,000 io tasks (50ms external call), MemoryMax=1G

| worker config | full_tps | p50 ms | p95 ms | p99 ms | peak RAM MiB |
|---|---|---|---|---|---|
| Celery prefork c=8 | 148.2 | 30,313 | 58,748 | 60,763 | 225.2 |
| Celery prefork c=16 | 291.3 | 13,812 | 25,234 | 26,349 | 406.6 |
| Celery gevent c=500 | 152.3 | 29,900 | 55,024 | 57,350 | 56.0 |
| rupy sync threads, 500 slots | 720.9 | 5,195 | 10,980 | 11,486 | 155.9 |
| **rupy async, 500 slots** | **3,600.2** | **390** | **555** | **567** | **170.7** |

- rupy async vs the best Celery config (prefork c=16): **12.4x throughput, 35x lower p50,
  at 42% of the RAM**. Against Celery gevent (its low RAM option): 23.6x throughput, 77x lower p50.
- rupy sync runs the byte identical `requests` task code as Celery: 4.9x prefork c=8.
- The rupy async 3,600 tps is machine bound (worker, Redis, mock API and driver share
  6 cores), not design bound; drain rate after enqueue ended was 13,700 tps.
- Celery prefork throughput is purely slot bound: c=8 gives 8/0.05 = 160 theoretical, it
  measured 148 to 164. Doubling throughput means doubling processes and RAM, linearly.

## S2: 2,000 cpu tasks (51ms PBKDF2), MemoryMax=1G

| worker config | full_tps | p50 ms | p95 ms | peak RAM MiB |
|---|---|---|---|---|
| Celery prefork c=6 | 73.6 | 12,013 | 23,936 | 164.5 |
| rupy cpu-workers=6 | 72.9 | 13,225 | 25,701 | 147.8 |

Parity within 1%, as designed: CPU bound work is core bound in any architecture. rupy's
line JSON pipe to its child executors costs about the same as Celery's billiard IPC.
Both reach ~62% of the 117.6 theoretical (6 cores also run Redis, the driver and the OS).

## S3: 10,000 io tasks under MemoryMax=512M (stress)

| worker config | full_tps | p99 ms | peak RAM MiB | OOM kills |
|---|---|---|---|---|
| Celery prefork c=8 | 145.9 | 59,989 | 220.3 | 0 |
| Celery gevent c=500 | 137.5 | 64,684 | 51.7 | 0 |
| rupy async, 1000 slots | 3,690.8 | 628 | 190.7 | 0 |

rupy ran 1,000 concurrent slots inside 512M using 37% of the cap; doubling its
concurrency from S1 added 20MiB. Honesty note: Celery prefork survived here because these
benchmark forks are bare (about 23MiB marginal per slot, see S4). In production each fork
carries your full app import (the 150 to 250MB per worker that motivated this project),
so c=8 alone typically needs 1.2 to 2GB and c=500 prefork is impossible, while rupy loads
the app once per process regardless of slot count.

## S4: idle RAM (worker started, no tasks, 20s settle)

| worker config | idle RAM MiB | marginal cost per additional slot |
|---|---|---|
| Celery prefork c=8 | 199.0 | ~21 MiB per slot (bare tasks; app size in production) |
| Celery prefork c=16 | 370.0 | " |
| Celery gevent c=500 | 43.1 | ~0 but throughput collapsed (see S1) |
| rupy (any io concurrency) | 147.1 | ~0 (500 to 1000 slots measured: +20MiB total) |

rupy idle = one Rust worker (~35MiB with embedded CPython, per its stats line) plus 6
default `rupy._exec` cpu children. io only deployments can run fewer cpu children.

## Client enqueue rate

rupy client published 4,500 to 7,100 tasks/s; Celery's publisher managed 1,330 to 1,560
(3 to 5x). Both single threaded from the driver process.

## Caveats

- Celery gevent underperformed its greenlet count badly (152 tps with 500 greenlets).
  Config was reliability equivalent to rupy (acks_late, prefetch 1); tuning
  `worker_prefetch_multiplier` up would trade delivery guarantees for throughput.
  Numbers are reported as measured under equivalent semantics.
- Six shared cores mean absolute numbers are conservative for rupy async; on real
  hardware with the API actually external, the gap grows.
- Single run per scenario (variance not characterized); each preceded by a warmup.

## Reproduce

```bash
wsl.exe -d Ubuntu-24.04 -e bash -lc \
  "cd /path/to/cauli/bench && bash setup.sh && bash runner.sh"
```

Raw per scenario JSON (all samples, percentiles, memory timelines): `bench/results/*.json`.
Logs: `bench/results/logs/`.
