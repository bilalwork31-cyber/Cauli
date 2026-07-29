# bench: Celery vs cauli benchmark harness

Honest, resource capped, reproducible benchmarks on WSL2 (Ubuntu-24.04,
6 cores, 11GB RAM, Python 3.12.3, Redis 7). Everything here runs INSIDE WSL;
files are edited on the Windows side.

## Files

| File | Purpose |
|---|---|
| `setup.sh` | creates the venv at `$HOME/rupy-bench-venv`, installs deps, installs the cauli package editable if `../py` exists (skips gracefully if not) |
| `runner.sh` | orchestrates the whole suite sequentially; optional scenario filter arg |
| `driver.py` | measurement engine: enqueue, wait, throughput, latency percentiles, memory sampling, stall/OOM detection, JSON output |
| `mock_api.py` | uncapped starlette+uvicorn mock external API on 127.0.0.1:8077 (`/io` = 50ms, `/io?ms=N` variable, `/health`) |
| `verify_api.py` | proves mock_api sustains >2000 rps so it is never the bottleneck |
| `common.py` | the two canonical workloads, imported by BOTH stacks |
| `calibrate.py` | picks `CPU_ITER` so one cpu task is ~50ms on this machine |
| `tasks_celery.py` | Celery app (broker db0, backend db1) |
| `tasks_cauli.py` | cauli app per PROTOCOL.md section 6 |
| `test_driver.py` | unit sanity for the percentile/latency math |
| `results/` | one JSON per scenario + `results/logs/` for every process log |

## Run everything

```
wsl.exe -d Ubuntu-24.04 -e bash -lc "cd /path/to/cauli/bench && bash setup.sh && bash runner.sh"
```

Subset: `bash runner.sh S1` (prefix match on scenario names), `bash runner.sh S3b`,
`bash runner.sh cauli` or `bash runner.sh celery` (stack match).

Env knobs: `BENCH_REDIS_PORT` (default 6390, never use 6379),
`CAULI_WORKER_BIN` (default `$HOME/rupy-target/release/cauli-worker`),
`BENCH_VENV`, `BENCH_DRIVER_TIMEOUT` (default 600s per scenario wait).

If the cauli binary or the cauli python package is missing, runner.sh logs
"cauli binary not found", writes a `{"status":"skipped"}` marker JSON and
continues with the Celery scenarios. Rerun `setup.sh` after `../py` appears.

cauli workers are launched with `--python $VENV/bin/python` and with
`PYTHONPATH` set to the venv site packages plus the bench dir, so the
embedded interpreter and the `python -m cauli._exec` cpu children can import
`cauli`, `tasks_cauli`, `common`, `requests` and friends.

## Memory caps: the validated systemd-run form

User scopes DO have the memory controller on this machine (delegated
controllers: cpu memory pids). The validated incantation is:

```
systemd-run --user --scope --unit=NAME -p MemoryMax=1G -p MemorySwapMax=0 -- <worker cmd>
```

`MemorySwapMax=0` is REQUIRED: WSL2 has 8G of swap and without it the kernel
pushes the capped worker into swap instead of enforcing the cap. Validated by
running a memory hog under `MemoryMax=100M MemorySwapMax=0`: it is OOM killed
by the kernel (exit 137), and the scope's `memory.events` shows `oom_kill 1`.
Root system scopes are NOT needed. The runner resolves the scope's cgroup as

```
/sys/fs/cgroup$(systemctl --user show bench-NAME.scope -p ControlGroup --value)
```

and reads `memory.current` (sampled every 250ms), `memory.peak` and
`memory.events` from it. Worker pid = first line of `cgroup.procs`.

## Scenarios

| Scenario | Task | N | Cap | Contenders |
|---|---|---|---|---|
| S1 io_10k_1G | io (50ms HTTP) | 10000 | 1G | celery prefork c8, c16; celery gevent c500; cauli sync io (conc 500, 64 threads); cauli async io (conc 500) |
| S2 cpu_2k_1G | cpu (~50ms pbkdf2) | 2000 | 1G | celery prefork c6; cauli cpu workers 6 |
| S3 io_10k_512M | io | 10000 | 512M | celery prefork c8; celery gevent c500; cauli async (conc 1000). Celery prefork is EXPECTED to OOM/thrash here; the driver survives it and records status stalled or worker_dead plus the completed count and oom_kill count |
| S4 idle_ram | none | 0 | 1G | each worker config started, 20s settle, `memory.current` recorded (per slot cost story) |

Each throughput scenario is preceded by a 200 task warmup run (not recorded).

## Fairness notes

- Same machine, same throwaway redis (port 6390), strictly sequential runs,
  `FLUSHALL` between scenarios, identical task code from `common.py` on both
  stacks, and the mock API runs uncapped for the whole suite (verified >2000
  rps, so it is never the bottleneck).
- CPU is uncapped for both stacks (same 6 cores, sequential runs, so CPU
  contention is identical by construction). Only memory is capped, identically.
- Celery runs production fair: `task_acks_late=True` and
  `worker_prefetch_multiplier=1` (matches cauli's ack after completion and
  admission gating), results stored on both stacks (`task_ignore_result=False`
  vs cauli `store_result=True`), `result_expires=3600` vs cauli
  `result_ttl=3600`, JSON serialization on both, no retries on either side
  (`max_retries=0` in tasks_cauli; Celery does not auto retry).
- Enqueue uses each stack's native `.delay()`; enqueue time is recorded and
  reported separately from the pure execution window.
- Latency per task, same definition on both stacks (completion minus enqueue):
  Celery: backend `date_done` (UTC) minus the driver's wall clock immediately
  before `.delay()`. cauli: `finished_at` from the result JSON minus the same
  driver wall clock (which coincides with envelope `enqueued_at`, stamped by
  the client inside `.delay()`). Same clock, same machine.
- The sync io workload uses plain `requests.get` (one connection per call) on
  both stacks. The cauli async task uses a minimal HTTP/1.1 keepalive pool on
  raw asyncio streams (`common._AsyncHTTPPool`, one pool per loop). DEVIATION
  from the original httpx plan, measured on this machine: `httpx.AsyncClient`
  tops out near 300 rps of client side loop CPU and anti scales with
  concurrency (143 rps at 100 in flight; 77 rps in the verify reference
  phase), which would benchmark httpx instead of the runtime. The pool client
  was measured at 5407 rps with 300 in flight against the 50ms endpoint.
  Sync vs async client behavior therefore differs by design; compare S1d
  (sync) against celery for the strict apples to apples read, S1e shows the
  async ceiling.
- mock_api sets TCP_NODELAY on accepted sockets (uvicorn h11 does not), since
  Nagle plus delayed ACK added a ~40ms stall per request on reused keepalive
  connections, which would have throttled every keepalive client to ~25 rps
  per connection. Fresh connection clients (the sync workload) were never
  affected. Details in mock_api.py.
- Throughput is reported both as `exec_tps` (N over the window from enqueue
  end to last result) and `full_tps` (N over first enqueue to last result).
- `cpu_call` is calibrated once on this machine via `calibrate.py`; the chosen
  `CPU_ITER` and its measured ms are recorded in `common.py`.

## Verification utilities

```
bash -lc "cd .../bench && $HOME/rupy-bench-venv/bin/python mock_api.py &"   # then:
$HOME/rupy-bench-venv/bin/python verify_api.py     # >2000 rps check
$HOME/rupy-bench-venv/bin/python calibrate.py      # CPU_ITER calibration
$HOME/rupy-bench-venv/bin/python -m pytest -q test_driver.py
```

For manual verification runs use redis port 6393 (`BENCH_REDIS_PORT=6393`) so
nothing collides with a running suite on 6390.
