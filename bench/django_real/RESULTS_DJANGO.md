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

## Symmetric topology, both frozen (raw SQL layer)

The fairest possible fight: same process count, same concurrency budget,
`gc.freeze()` applied to BOTH stacks, identical raw data layer
(DJ_DATA_LAYER=raw: one-statement SKIP LOCKED claim, redis intent locks /
sent flags / attempts, outcomes LPUSHed to redis, bg persisters applying
them with raw executemany - the send hot path touches no Postgres). This
isolates architecture from configuration. Same chaos recipe as above: 100
campaigns / 1M recipients, uncorked dispatcher, 10-minute leases, 5% Graph
errors at 200-500 ms, all bg noise live, 60 s warmup + exactly 600 s window,
fresh schema + reseed per config, no memory caps.

Three configs, sequential:

- (a) **SYM_celery_frozen**: ONE Celery invocation, ALL queues via -Q,
  prefork -c 6, acks_late, prefetch 1. A guarded hook
  (CAULI_BENCH_GC_FREEZE=1 in celery_app_django) fully imports the Django
  app in the prefork MASTER, warms lazy imports (psycopg via
  ensure_connection, then close_all - no live connection crosses fork()),
  then `gc.collect(); gc.freeze()` before the 6 children fork. Each child
  keeps the production ThreadPoolExecutor(15) per batch.
- (b) **SYM_cauli_fork**: ONE cauli worker, every task kind="cpu"
  (DJ_CAULI_KIND=cpu): `--cpu-workers 6 --cpu-child-threads 15`. The
  fork-server parent (PROTOCOL §5.1) imports the app once, `gc.collect() +
  gc.freeze()`, forks 6 children at **1.4 MiB private each**; each child
  runs 15 in-flight tasks with DJ_SEND_POOL=15 - the same 6 GILs and the
  same per-child 15-thread budget as (a).
- (c) **SYM_cauli_async**: ONE cauli worker, the one-process io-lane raw
  variant (`--io-concurrency 40 --io-threads 48 --cpu-workers 1`,
  DJ_SEND_POOL=240) - the DJR real_raw cauli leg that was interrupted
  before completing, now measured.

| metric | (a) Celery -c 6 frozen | (b) cauli fork pool 6x15 | (c) cauli one-process io |
|---|---|---|---|
| sends in window | 53,793 | **126,347** | 30,095 |
| sends/s | 89.7 | **210.6 (2.35x)** | 50.2 |
| sends per 10 s (min/mean/max) | 720 / 897 / 1075 | 1982 / 2106 / 2231 | 150 / 502 / 1658 |
| p50 / p95 recipient wait | 352.9 s / 622.6 s | 357.8 s / 629.2 s | 319.3 s / 632.8 s (all timebox-shaped) |
| duplicates | **0** | **0** | **0** |
| SendLog rows == sent flags | 58,499 == 58,499 | 137,903 == 137,903 | 37,728 == 37,728 |
| failed / skipped | 0 / 0 | 0 / 0 | 0 / 0 |
| http retries absorbed | 3,152 | 7,283 | 1,990 |
| persist lag p50 / p95 | 402 ms / 1.32 s | 2.11 s / 10.7 s | 5.65 s / 65.0 s |
| cgroup peak RAM | **217.7 MiB** | 361.7 MiB | 241.0 MiB |
| OS processes | 7 (master + 6 forks) | 8 (worker + parent + 6 forks) | 3 (worker + parent + idle fork) |
| sum of per process peak RSS | 590.9 MiB | 661.2 MiB | 321.8 MiB |
| sum of per process PRIVATE rss (end) | 160.9 MiB | 322.7 MiB | 193.2 MiB |
| per child PRIVATE rss | 23.3-23.8 MiB | 38.8-52.5 MiB (1.4 MiB at fork) | n/a (idle child 1.5 MiB) |
| claimed_total (lease reclaims) | 1.69M | 1.88M | 1.47M |

Prior prod-topology rows for contrast (same raw layer / same ORM recipe):

| | sends/s | cgroup peak | processes | sum peak RSS | sum USS |
|---|---|---|---|---|---|
| DJR_celery_raw (prod topology, 7 invocations, no freeze) | 77.1 | 971.6 MiB | 24 | 1,783.5 MiB | 657.0 MiB |
| DJ_celery (ORM layer, prod topology) | 73.6 | 923.1 MiB | 22 | 1,668 MiB | 622 MiB |
| DJ_rupy (ORM layer, one process sync) | 98.9 | 441.7 MiB | 2 | 471 MiB | 436 MiB |

Background noise stayed live on all three: webhook inbox 500/500 done
(34/34/32 injected failures retried, 0 dead), ghost backfill 537 / 422 /
271 paced Graph calls with BackfillJob rows written throughout.

### The per fork PRIVATE rss story (CoW proof, both stacks)

Private_ (smaps_rollup) is what a process does not share with anyone - the
copy-on-write cost of a fork. gc.freeze works, on BOTH stacks: Celery's
frozen children end the run at ~23.5 MiB private each (the prod-topology
children carried 69-98 MiB RSS each, 1.67-1.78 GiB summed), and cauli's
fork children START at 1.4 MiB private and end at 39-52 MiB after 10
minutes of 15-way concurrent work. The fleet collapses accordingly:
Celery 971.6 -> 217.7 MiB cgroup peak for MORE throughput (89.7 vs 77.1/s;
the prod topology wasted its 21 processes on idle dedicated queues).
cauli_fork spends 361.7 MiB - more than frozen Celery, because each child
holds 15 in-flight tasks (its own Postgres connections and heap for 15
tasks vs Celery's 1) - and returns 2.35x the throughput: 1.7 MiB per
send/s vs 2.4 for frozen Celery.

### Where the remaining differences come from

- (b) vs (a) is the architecture gap with configuration equalized: same 6
  GILs, same 15 threads per process, same frozen master, same broker, same
  task code. Celery binds one task to one process slot; a 50-send batch,
  a ghost_job iteration sleeping 1 s, or an idle persist poll each occupy
  a whole GIL. cauli's children multiplex 15 tasks per GIL, so sends keep
  flowing while slow/bg tasks overlap - 90 in-flight tasks vs 6 on the
  same processes. Broker mechanics differ too: cauli children receive work
  over a unix socket from the Rust worker (redis streams consumed once, in
  batches); each Celery slot round-trips the redis broker per task at
  prefetch 1.
- (c) shows the one-process sync-thread path is the WRONG lane for this
  workload at scale, and honestly: it matched its smoke (115/s) for the
  first ~70 s, then collapsed to ~40-70/s when the 12 persist chains began
  draining their 8.8k backlog inside the same GIL as 240 send threads +
  claims, and never recovered (per 10 s: 1658 down to 150). One GIL is the
  ceiling and every subsystem shares it. The ORM run above (98.9/s)
  dodged this only because its persist batches were lighter. The fork
  pool exists precisely to end this class of tuning: (b) needed none.
- Supervisor RAM: cauli carries a Rust worker (74.5 MiB peak RSS, 60.5
  private, embedded CPython) + a 63 MiB frozen parent; Celery's master IS
  a full Django process (90 MiB peak). Roughly a wash at this app size;
  cauli's overhead is constant while Celery's master grows with the app.

### Fairness notes and deviations (symmetric runs)

- msgspec was installed into the bench venv for these runs: the cauli
  client auto-detects it for envelope encode/decode; Celery kept its own
  kombu json serializer (its default fast path). Envelope codecs are
  therefore not identical - noted as a (small) cauli-side advantage.
- Celery ran WITHOUT max-tasks-per-child (prod topology used 1000 on the
  long queue): recycling would re-fork from the frozen master cheaply on
  both stacks, but zero recycling is the cleaner CoW measurement.
- 12 self-chaining persist tasks on all three configs (raw-layer default),
  competing for the same worker slots on both stacks - no dedicated
  persist worker anywhere, unlike the prod-topology raw run.
- PRIVATE rss is sampled from /proc/PID/smaps_rollup every 60 s and at
  shutdown; "end" values are the final sample, taken under load just
  before SIGTERM. peak_private is the max of those samples.
- (b)'s per-page semaphores live per child (3 per page per process), same
  as every prefork Celery child - identical to how the prod topology
  behaved; per-process global send pools likewise (15 per process both).
- claimed_total > 1M on all three: 10-minute leases from the first ticks
  expire inside the 11-minute run, orphans reclaimed (production
  behavior). lock_skips=0, already_sent=0, dup=0 everywhere.
- Wait percentiles remain timebox-shaped (uncorked dispatcher, drain-rate
  proxy); sent recipients only. Single run per config, 6 shared cores.
- Mini smokes (2 min, 20k) passed first: (a) 92.7/s, (b) 114.8/s with a
  full 20,000 == 20,000 drain, dup=0 both. (c) reused the already-proven
  real_raw wiring (its smoke: 115.3/s).

## Reproduce

```bash
# one-time (as root): creates bench_django, max_connections=400
wsl.exe -d Ubuntu-24.04 -u root -e bash -lc "bash /mnt/d/dev/projects/boring/rupy/bench/django_real/pg_setup_django.sh"
# smoke (2 min window, 20k recipients), then the real thing (10 min, 1M)
wsl.exe -d Ubuntu-24.04 -e bash -lc "bash /mnt/d/dev/projects/boring/rupy/bench/django_real/runner_django.sh smoke"
wsl.exe -d Ubuntu-24.04 -e bash -lc "bash /mnt/d/dev/projects/boring/rupy/bench/django_real/runner_django.sh real"
# raw data layer variants (DJR_*): smoke_raw / real_raw
# symmetric topology, both frozen (SYM_*): sym_smoke / sym
wsl.exe -d Ubuntu-24.04 -e bash -lc "bash /mnt/d/dev/projects/boring/rupy/bench/django_real/runner_django.sh sym"
```

Raw JSON (timelines, per process RSS tables, bg counters, memory samples):
bench/django_real/results/DJ_*.json. Logs: results/logs/.
