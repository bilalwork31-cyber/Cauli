# Campaign mimic benchmark: production Meta-Chat workload on Celery vs rupy

Workload faithfully mimics the production Django+Celery campaign pipeline (mapped from
run_celery_campaigns.sh / run_bg_fill.sh and their task code): dispatcher claiming
recipients with an atomic SKIP LOCKED analog and 10 minute leases, send batches of 50
through a thread pool of 15 with 3 concurrent sends per page, per recipient one POST to a
fake Graph API (200 to 500ms, injected 429/500 errors), production retry backoff
`min(90, 8*2^(n-1)) + rand(1,15)`s with 3 attempts, intent locks against double sends,
plus background fill noise (ghost backfill pacing loops, webhook inbox drain) running
concurrently. Celery runs its exact production process topology (dispatch solo,
campaign_long c=4, campaign_short c=2, default c=2, all prefork, acks_late, prefetch 1)
in one 1G cgroup; rupy runs ONE worker process in an identical 1G cgroup. Same shared
task code both sides. Zero duplicate sends everywhere (enforced and counted).

## A: exact production knobs (4,000 recipients, 20 pages, 1s per page pacing, 2% errors)

| stack | sends/s | drain | p50 wait | peak RAM |
|---|---|---|---|---|
| Celery production topology | 13.8 | 289.5s | 1.70s | 331.9 MiB |
| rupy sync (same threaded code) | 13.6 | 293.2s | 2.03s | 59.4 MiB |
| rupy async | 13.7 | 291.1s | 1.79s | 46.3 MiB |

The production dispatcher quota (200 per 15s tick) is the bottleneck, so throughput ties
by design. The difference is 5.6 to 7.2x less RAM for identical work, and that is with
bare bench tasks; production forks each carry the full Django app.

## B: dispatcher uncorked (10,000 recipients, 1 campaign, 50 pages, 0.2s pacing)

| stack | sends/s | drain | p50 wait | p99 wait | peak RAM |
|---|---|---|---|---|---|
| Celery production topology | 78.6 | 127.2s | 51.1s | 103.4s | 334.3 MiB |
| rupy sync | 225.7 | 44.3s | 11.9s | 21.8s | 147.6 MiB |
| rupy async | 237.2 | 42.2s | 9.6s | 17.7s | 58.7 MiB |

Celery caps at its campaign_long children (4 processes x 15 threads); rupy runs the full
per page concurrency budget in one process: 3x throughput, 5x lower median wait.

## C: full throughput chaos (100 campaigns, 445,180 recipients, no dispatcher limits, 200 pages, 5% errors, send -> Redis -> Postgres persist stage)

| stack | outcome | sends/s | drain | sent | in Postgres | dup | peak RAM | OOM |
|---|---|---|---|---|---|---|---|---|
| Celery production topology + persist worker | **stalled at 90 min timeout** | 78.7 | >5400s | 425,253 / 445,180 (95.5%) | 424,704 | 0 | 443.1 MiB | 0 |
| rupy, one process | **completed** | 744.7 | 597.8s (~10 min) | 445,180 / 445,180 | 445,180 (exact match) | 0 | 193.0 MiB | 0 |

- Celery never finished: at ~79 sends/s its projected drain is ~94 minutes. Its p50 queue
  wait was 44.7 minutes, p95 85.5 minutes. This is the same ~79/s ceiling as scenario B:
  the topology is the limit, and scaling it means adding forks and RAM linearly.
- rupy pushed 9.5x the throughput through one process at 193MiB, absorbed 23,457 transient
  Graph errors via in process retries, delivered every recipient exactly once, and the
  Postgres table matched the send count exactly.
- Persist stage: Celery's persister kept lag at ~290ms p50 (it had only ~79 rows/s to
  persist). rupy's 745 sends/s outran the 2 persister tasks (LPOP 500 batches); persist
  lag reached p50 86s and the backlog cleared at the end. For lower lag at this rate, run
  more persister tasks or bigger batches; end state integrity was exact either way.
- Both stacks: failed=0 (all injected errors recovered within 3 attempts), dup=0, oom=0.

## Caveats

- 6 shared cores host workers, Redis, Postgres, fake Graph and the driver; absolute
  numbers are conservative, relative numbers are the story.
- Bench tasks import no real Django app, which favors Celery's RAM numbers: production
  forks each load the app (the 250MB per worker that motivated this), rupy loads it once.
- Per page semaphores are per process (mirrors production prefork exactly): Celery's 4
  children can each run 3 per page, rupy's single process enforces a strict global 3.
- Single run per scenario.

## Reproduce

```bash
# A/B
wsl.exe -d Ubuntu-24.04 -e bash -lc "bash /mnt/d/dev/projects/boring/rupy/bench/campaign/runner_campaign.sh"
# C (Postgres role/db set up once via pg_setup.sh)
wsl.exe -d Ubuntu-24.04 -e bash -lc "BENCH_DRIVER_TIMEOUT=5400 bash /mnt/d/dev/projects/boring/rupy/bench/campaign/runner_c.sh"
```

Raw JSON with timelines and histograms: bench/campaign/results/*.json. Logs: results/logs/.
