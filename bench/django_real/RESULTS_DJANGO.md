# django_real: campaign chaos on a REAL Django app, 10 minute time box, no caps

The most production realistic scenario in the suite. Same Meta-Chat campaign
chaos recipe as bench/campaign run C, but the storage layer is a real Django
ORM app on Postgres and ALL mapped background workers run concurrently. Both
stacks execute the identical shared task code (tasks_shared.py) against the
identical schema, seeds and fake Graph API.

## Environment

- WSL2 Ubuntu 24.04, 6 shared cores, 12 GB RAM (workers, Redis, Postgres,
  fake Graph and the driver all share them; absolute numbers conservative,
  relative numbers are the story).
- Django 6.0.7 + psycopg 3.3.4 on Postgres 16.14, database `bench_django`,
  max_connections=400 (pg_setup_django.sh). Full standard Django install:
  admin, auth, sessions, contenttypes, messages, staticfiles in
  INSTALLED_APPS; nothing inflated beyond `startproject` + one app.
- Celery 5.x prefork, acks_late, prefetch 1. rupy worker binary from
  ~/rupy-target/release (one process, all queues).
- Throwaway redis on 6396 (broker db0, celery backend db1, bench counters
  db3, persist buffer db4). Fake Graph on 8078: 200 to 500 ms per send, 5%
  injected 429/500.
- systemd user scopes WITHOUT MemoryMax (accounting only), MemorySwapMax=0.

## Workload

- 100 campaigns, 1,000,000 recipients (10,000 each), 200 pages global
  round robin, seeded via ORM bulk_create into a freshly truncated schema
  before each stack run (56 s).
- Dispatcher uncorked (no 200/15 s quota): every 5 s tick claims all due rows
  per campaign with `select_for_update(skip_locked=True)` + 10 minute leases
  (production LEASE_MS=600000), chunks into batches of 50.
- Send path per recipient (identical both stacks): per page semaphore (3
  concurrent per page), DB intent lock (conditional UPDATE on lock_until_ms),
  attempts increment, sent flag check, tripwire UPDATE WHERE sent_flag=false
  before every actual POST, up to 1+3 in-process HTTP retries (1/2/4 s), then
  production backoff min(90, 8*2^(n-1)) + rand(1,15) s, max 3 attempts.
- Persist stage keeps the run C shape: success pushes a record to redis
  `results_raw` FIRST, then sets the sent flag; two self-chaining persister
  tasks LPOP 500 and bulk_create into SendLog with ignore_conflicts.
- Background noise concurrently on BOTH stacks, bgfill_common paces: ghost
  backfill chain (50 x {GET /conversations + 1 s sleep} per run, writing a
  BackfillJob ORM row per iteration) and webhook inbox drain each tick (claim
  50 WebhookInboxItem rows FOR UPDATE SKIP LOCKED, 10 ms each, 5% failures,
  2^attempts backoff, dead at 5).
- 60 s warmup (not counted), then EXACTLY 600 s measured via marker
  timestamps; sends counted by sent_at in [marker, marker+600). Hitting the
  time box is status `timebox_ok`. Graceful stop (SIGTERM: celery warm
  shutdown / rupy drain), then a finalize pass drains leftover results_raw
  and audits Postgres.

## Topologies

- Celery, production shape + bg fill workers, all prefork on one Django app:
  dispatch (solo), campaign_long c=4 (max-tasks-per-child 1000),
  campaign_short c=2, default c=2, persist c=2, backfill_heavy c=2,
  webhook_ingest c=2. 22 OS processes observed, every one a full Django
  import.
- rupy: ONE worker process, ALL queues
  (default,dispatch,campaign_short,campaign_long,backfill_heavy,
  webhook_ingest,persist), `--app rupy_app_django:app` calling
  `django.setup()` once; `--io-concurrency 40 --io-threads 48 --batch 8`,
  send pool 240 threads (DJ_SEND_POOL).

## Results (600 s measured window, 1M backlog, both `timebox_ok`)

| metric | Celery prod topology | rupy one process | delta |
|---|---|---|---|
| sends in window | 44,150 | 59,342 | +34% |
| sends/s | 73.6 | 98.9 | 1.34x |
| sends per 10 s (min/mean/max) | 590 / 736 / 974 | 452 / 989 / 1243 | steadier vs burstier |
| p50 recipient wait | 358.7 s | 363.9 s | saturated (see notes) |
| p95 recipient wait | 622.3 s | 613.6 s | saturated |
| duplicates | **0** | **0** | enforced + counted |
| SendLog rows == sent flags | 48,557 == 48,557 | 62,400 == 62,400 | exact both |
| failed / skipped / OOM | 0 / 0 / 0 | 0 / 0 / 0 | |
| http retries absorbed | 2,606 | 3,303 | 5% error injection |
| persist lag p50 / p95 | 217 ms / 791 ms | 5.9 s / 12.9 s | both bounded, backlog drained |
| cgroup peak RAM | **923.1 MiB** | **441.7 MiB** | 2.1x |
| OS processes | 22 | 2 (worker + cpu child) | |
| sum of per process peak RSS | **1,668 MiB** | 471 MiB | 3.5x |
| sum of per process USS | 622 MiB | 436 MiB | |

Background noise ran live on both stacks: webhook inbox 500/500 processed
(celery 22 injected failures retried to done, rupy 26; 0 dead), ghost
backfill 550 paced Graph calls / 11 completed runs on celery vs 376 / 7 on
rupy, BackfillJob rows written every second, ORM state confirming both.

### The per fork RSS story (the headline)

Every one of Celery's 21 working processes carries the full Django app:
per process peak RSS 69 to 98 MiB (typical child ~80 MiB, USS 8 to 45 MiB
on top of shared pages), summing to 1.67 GiB peak RSS / 923 MiB cgroup peak
for one modest app. This bench app is a *minimal* real Django install; the
production app this mimics runs 150 to 250 MiB per fork, so the same
topology there is a 3 to 5 GiB fleet. rupy loads the identical app ONCE:
409 MiB worker + 62 MiB cpu child, 442 MiB cgroup peak, while also doing
34% more work.

## vs run C (same chaos, redis fake store instead of Django ORM)

| | run C (redis store) | django_real (real ORM) |
|---|---|---|
| Celery sends/s | 78.7 | 73.6 |
| rupy sends/s | 744.7 (async) | 98.9 (sync ORM) |
| Celery peak RAM | 443.1 MiB (1G cap) | 923.1 MiB (no cap) |
| rupy peak RAM | 193.0 MiB | 441.7 MiB |
| duplicates | 0 / 0 | 0 / 0 |

- Celery's ~75/s ceiling is unchanged: its topology (6 send slots of 15
  threads) is the limit in both worlds, and the real ORM cost hides inside
  the 200-500 ms Graph latency. What changed for Celery is RAM: bare tasks
  cost 443 MiB, the real Django app costs 923 MiB cgroup / 1.67 GiB summed
  RSS for the same ~75/s.
- rupy dropped from 745/s to 99/s because the Django ORM is synchronous
  Python executed under ONE GIL: at ~6-8 ms of Python per send, one process
  saturates around ~150/s regardless of thread count. Celery buys extra GILs
  with forks (6 send processes); rupy's one-process design intentionally does
  not. Run C's rupy number used the async redis store where the GIL is
  barely touched. That is the honest trade this scenario exposes: with a
  sync ORM in the hot path, rupy still wins (+34% throughput at 2.1x to 3.5x
  less RAM, with persist and bg noise inside the same process), but the 9.5x
  of run C is an async-storage number, not a Django ORM number.

## Fairness notes and deviations

- Identical shared task code, schema, seed, knobs, noise and fake Graph for
  both stacks; celery ran first, fresh schema + redis flush between runs.
- Sync send path on BOTH stacks (run C rupy used async): Django's ORM is
  synchronous; rewriting storage as async would redesign the app.
- The per batch ThreadPoolExecutor(15) of run C became one process-global
  executor with identical caps (15 per Celery child = same as per batch under
  prefetch 1; per page 3 everywhere; rupy budget 240). Long-lived threads
  keep Django's per-thread Postgres connections persistent instead of
  churning 15 fresh connections per batch, which no real deployment would
  tolerate; Postgres max_connections raised to 400 (rupy run sits at ~300
  connections, celery ~110).
- rupy `--io-concurrency 40` (run C used 1000): sync batches block their
  admission slot, and hundreds of admitted blocked batches starved the short
  persist/dispatch tasks (caught in smoke, persisted=0). 40 slots on 48 sync
  threads recycle in seconds; the backlog stays in redis streams. rupy's
  persist lag (p50 5.9 s vs celery 217 ms) is the remaining cost of sharing
  one process with 40 in-flight send batches; the backlog stayed bounded
  (~600 rows) and drained.
- Recipient wait p50/p95 is timebox-shaped on both stacks: the uncorked
  dispatcher claims the full 1M in the first ticks, so wait ~ queue position
  / drain rate and both stacks sit near window midpoint. It discriminates
  drain rate, not latency, in this scenario.
- claimed_total exceeded 1M on both (celery 1.47M, rupy 1.87M): 10 minute
  leases from the first ticks expire inside the 11 minute run and orphans are
  reclaimed, exactly the production lease-reclaim behavior. Guards held:
  lock_skips=0, already_sent=0, dup=0.
- Wait percentiles measure sent recipients only (SendLog rows in window);
  the 0.94M recipients still queued at cutoff have no wait sample. Single
  run per stack. Same 6 shared cores host everything.
- Mini smoke (2 min, 20k) passed both stacks first: celery 75.6/s / rupy
  128.4/s, dup=0, integrity exact, full drain to SendLog 20,000 == 20,000.

## Reproduce

```bash
# one-time (as root): creates bench_django, max_connections=400
wsl.exe -d Ubuntu-24.04 -u root -e bash -lc "bash /mnt/d/dev/projects/boring/rupy/bench/django_real/pg_setup_django.sh"
# smoke (2 min window, 20k recipients), then the real thing (10 min, 1M)
wsl.exe -d Ubuntu-24.04 -e bash -lc "bash /mnt/d/dev/projects/boring/rupy/bench/django_real/runner_django.sh smoke"
wsl.exe -d Ubuntu-24.04 -e bash -lc "bash /mnt/d/dev/projects/boring/rupy/bench/django_real/runner_django.sh real"
```

Raw JSON (timelines, per process RSS tables, bg counters, memory samples):
bench/django_real/results/DJ_*.json. Logs: results/logs/.
