# cauli benchmark suite

This measures specific, falsifiable claims about cauli against Celery,
taskiq, arq and Dramatiq — not a generic "which is faster" contest.
Covers raw dispatch, memory, CPU-bound work, crash/reliability behavior,
and real framework code (Django's ORM, SQLAlchemy's async ORM), not just
a bare no-op. Start here:

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

**Claim 5's pgbouncer-backed numbers need one extra step**, not covered by
`setup.sh`: a pgbouncer instance in front of the same Postgres role/db,
`pool_mode = transaction`, pointed at from `BENCH_PG_DSN` when running
those specific lanes. Config used for the numbers in RESULTS.md:

```ini
[databases]
bench = host=127.0.0.1 port=5432 dbname=bench

[pgbouncer]
listen_addr = 127.0.0.1
listen_port = 6433
auth_type = plain
auth_file = userlist.txt   # one line: "bench" "bench"
pool_mode = transaction
max_client_conn = 3000
default_pool_size = 350
min_pool_size = 250
```

If you point a psycopg3-based lane at pgbouncer, also pass
`prepare_threshold=None` in the connection kwargs — see RESULTS.md's
"Second bug found and fixed" note under Claim 5 for why (server-side
prepared statements and transaction-pooling mode don't mix). Already
applied in `tasks_cauli_sync_pg.py` / `tasks_cauli_async_pg.py`.

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
| `soak_driver.py` | Sustained load + periodic PSS sampling for the 24-48h soak test |
| `raw_asyncio_enqueue.py` / `raw_asyncio_worker.py` | The no-framework ceiling every lane is bounded by |
| `djapp/` / `django_settings.py` | Minimal Django app (unmanaged model onto the shared `bench_io` table) for the Django ORM lane |
| `sqla_models.py` | SQLAlchemy 2.0 declarative model onto the same table, for the async ORM lane (Core and ORM both go through this) |
| `tasks_cauli_sync_pg.py` / `tasks_cauli_async_pg.py` | Raw psycopg3 PG lanes — also the pgbouncer-safe reference, `prepare_threshold=None` set unconditionally |
| `Dockerfile` / `docker-compose.yml` / `docker-init.sql` | One-command reproduction (unvalidated on this dev machine — see RESULTS.md) |

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
