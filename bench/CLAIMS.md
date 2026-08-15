# Claims under test

Every benchmark in this suite exists to support or refute one of these. If a
measurement doesn't map to a claim below, it doesn't belong in this suite.

1. **Memory per unit of concurrency.** N concurrent in-flight I/O tasks cost
   less RAM in cauli (one process, many slots) than in Celery (one OS process
   per worker slot). Measured: RSS at 100 / 1,000 / 10,000 in-flight tasks,
   cauli vs Celery (all worker processes summed).

2. **Dispatch overhead is small and roughly constant.** The per-task cost of
   enqueue + fetch + dispatch + ack (excluding the task body itself) is low
   and doesn't grow with concurrency. Measured: no-op task throughput
   converted to per-task µs, plus a redis-benchmark baseline so a claim never
   gets confused with "this is just measuring Redis."

3. **True multicore for CPU-bound work.** `kind="cpu"` tasks get real
   parallelism via forked children, not GIL-serialized threads. Measured:
   CPU-bound throughput at 0.5/2/10/50ms per task, cauli vs Celery prefork.
   Celery prefork is a strong, mature competitor here — this is the workload
   where cauli is most likely to lose, and it is reported honestly either way.

4. **A mixed I/O + CPU-burst workload is survivable, and the failure mode is
   fixable, not silent.** Original hypothesis: a naive single-process async
   design (cauli's async lane, GIL shared across every in-flight task) stalls
   every other in-flight task during a CPU burst; routing that burst to
   `kind="cpu"` should fix it, and Celery prefork (one OS process per task)
   should be structurally immune. First measurement (20s run, 100 light/s +
   one 50ms poison burst every 3s, near-burst vs baseline p99 latency):
   naive cauli async 18.0x, `kind="cpu"`-fixed cauli async 4.0x, Celery
   prefork **14.5x — not immune**, contrary to the hypothesis. Likely cause:
   this is a 6-core box where Celery's processes, Redis, Postgres and the
   harness itself all compete for the same cores, so a CPU burst can starve
   OS scheduler fairness across processes even without a shared GIL — a
   different mechanism than the GIL-stall, similar symptom. Not yet
   disambiguated from genuine architecture on isolated cores (planned:
   rerun under `taskset` pinning). Reported as measured, not as assumed.
   Also see: the failure-mode question this claim invites — what happens
   when a task segfaults? Answered directly, not left for someone else to
   discover (see chaos/ section of RESULTS.md: blast radius of a
   segfaulting ctypes call).

## What is explicitly NOT claimed

- Priorities, chains, chords: not supported (see PROTOCOL.md §11), not
  benchmarked.
- Nothing here claims cauli is faster in every workload. CPU-bound at large
  task sizes is expected to be parity or a loss against Celery prefork; it is
  reported anyway.
