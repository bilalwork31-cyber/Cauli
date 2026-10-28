# Benchmarks

Each benchmark backs one claim about cauli against the Python task queues
people choose between: Celery, taskiq, Dramatiq and arq. Numbers live in
[RESULTS.md](RESULTS.md).

| Benchmark | Claim it tests |
|---|---|
| `dispatch` | Per task overhead (fetch, dispatch, execute, ack) is lower than every competitor, each at its best measured configuration |
| `cpu` | `kind="cpu"` tasks use every core: CPU bound throughput at 0.5, 2, 10 and 50 ms per task, against Celery prefork |
| `memory` | N concurrent I/O tasks cost less RAM than one process per slot: PSS at 100, 1,000 and 10,000 in flight |
| `chaos` | A `kill -9` loses no task. A segfault takes down only the process it happens in |
| `web` | Enqueueing inside a real Django or FastAPI request is cheap, and every request's task runs exactly once |
| `free-threaded` | On free threaded CPython the sync lane runs CPU bound work in parallel without forking |

Not claimed: that cauli wins every workload. At 50 ms CPU bound tasks every
framework is bound by the cores, and the table says so.

## Run

From the repository root, on Linux:

```bash
python -m bench dispatch        # paired rounds, 95% interval per lane
python -m bench cpu
python -m bench memory --sizes 100,1000,10000
python -m bench chaos
python -m bench web             # needs k6
python -m bench free-threaded   # needs a second interpreter and worker build
```

Every command prints a Markdown table. `--help` on any of them lists its size
knobs; `--scale 0.1 --rounds 2` gives a quick smoke run of the drain
benchmarks.

## Method

- **Drain rate.** Preload N tasks with no worker running, start the worker,
  take the completion slope between 10% and 90% done. A live producer racing
  the consumer stops measuring the worker once the worker is faster.
- **Paired rounds.** Every lane runs once per round, in a rotated order, and
  each is reported as the mean of its per round ratio to the first lane with
  a 95% interval. One config on this shared box drifted from 19,532 to 12,540
  tasks/s within a session, so a block design reports drift as a difference.
- **CPU columns.** Cores used by the worker's process tree and by Redis over
  the same window as the rate. A Redis near 1.0 core means the row measured
  Redis.
- **PSS, not RSS.** Forked children share pages; RSS counts them once per child.
- **Every worker process dies before the next run.** Processes run in their
  own sessions and are SIGKILLed as a tree. Celery's warm shutdown on SIGTERM
  can wait forever, and one leaked pool skews every later run.

## Prerequisites

- Linux (cauli-worker), Redis on port 6395 started for the bench only:
  `redis-server --port 6395 --save '' --appendonly no --daemonize yes`.
  Every run flushes it.
- PostgreSQL with role and database `bench`, password `bench`, for `web`.
- k6 on `PATH` (or `K6_BIN`) for `web`.
- A release worker: `cargo build --release --bin cauli-worker` in `worker/`.
- Two venvs. arq pins `redis<6`, which conflicts with the rest:

```bash
uv venv ~/bench-venv && uv pip install --python ~/bench-venv/bin/python -e py -r bench/requirements.txt
uv venv ~/arq-venv && uv pip install --python ~/arq-venv/bin/python -r bench/requirements-arq.txt
```

| Variable | Default | Used by |
|---|---|---|
| `CAULI_WORKER_BIN` | `cauli-worker` | every benchmark |
| `ARQ_PYTHON` | required | `dispatch`, `chaos` |
| `GIL_PYTHON`, `GIL_WORKER`, `FT_PYTHON`, `FT_WORKER` | required | `free-threaded`: each interpreter with a worker built against it |
| `BENCH_REDIS_URL` | `redis://127.0.0.1:6395/0` | every benchmark |
| `BENCH_PG_DSN` | `postgresql://bench:bench@127.0.0.1:5432/bench` | `web` |
| `BENCH_WEB_PORT` | `8100` | `web` |
| `BENCH_LOG_DIR` | `/tmp` | worker logs, one file per benchmark |

Run the harness with the bench venv's Python. Workers get the clone's `py/`
ahead of any installed cauli, so the code under test is the code in the tree.
