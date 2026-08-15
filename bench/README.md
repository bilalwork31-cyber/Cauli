# cauli benchmark suite

This measures specific, falsifiable claims about cauli against Celery,
taskiq, arq and Dramatiq — not a generic "which is faster" contest. Start
here:

1. **[CLAIMS.md](CLAIMS.md)** — the claims under test. If a number in this
   directory doesn't map to one of these, it doesn't belong here.
2. **[RESULTS.md](RESULTS.md)** — the measurements, methodology, environment,
   and an explicit "not yet done" section. Read this before trusting any
   number, including the ones that favor cauli.

## Reproduce it

```bash
./setup.sh          # builds cauli-worker fresh, pinned venv, dedicated redis/pg
python3 campaign.py --reps 3
```

Requires Linux (cauli-worker's constraint, not this harness's), Redis,
PostgreSQL, and Rust/Cargo. `setup.sh` never touches a Redis or Postgres
instance you already run — everything here is on its own port/role/database.

## What's in here

| File | Purpose |
|---|---|
| `common.py` | Shared Redis URL / key constants every lane imports |
| `workloads.py` | Shared task bodies (`cpu_burn`, Postgres insert SQL) so every framework runs identical work |
| `tasks_<framework>_<workload>.py` | One task module per (framework, workload) combination |
| `enqueue.py` | Preloads N tasks for a lane with no worker running (drain-rate setup) |
| `monitor.py` | Polls a completion counter, computes the mid-80%-slope drain rate |
| `run.sh` | Orchestrates one measurement: flush, enqueue, start worker, monitor, clean up |
| `campaign.py` | Runs the pinned final configs from RESULTS.md, N reps, prints a summary table |
| `latency_producer.py` / `latency_report.py` | Open-loop load generation + HdrHistogram percentiles |
| `mixed_driver.py` / `mixed_report.py` | Adversarial I/O + CPU-burst workload and its analysis |
| `chaos_driver.py` | `kill -9` mid-run, measure data loss / duplicates / recovery time |
| `segfault_driver.py` | Segfault blast-radius: what dies, what survives, what auto-recovers |
| `memory_report.py` | Sums PSS (not RSS — see RESULTS.md for why) across a process group |

## Methodology, in one paragraph

Throughput is drain-rate (preload with no worker running, then measure —
never a live producer racing the consumer, which goes invalid the moment the
worker outpaces it). Latency is open-loop (fixed send schedule regardless of
completion — a closed-loop generator hides collapse under load). Every
framework is tuned to its own optimum, not left at defaults, with the
default-vs-tuned gap reported where it's large enough to matter. Delivery
guarantees are compared at matched configuration, not across guarantee
levels — Celery's default (early-ack) and a properly-configured version
(`acks_late`) are both reported, labeled, rather than picking whichever one
makes the comparison flattering. Full detail in RESULTS.md.

## Status

Actively being extended. See RESULTS.md's "Not yet done" section for what's
missing and why, rather than treating silence as "already covered."
