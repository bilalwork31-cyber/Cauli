# Benchmark results

One session, one machine, every benchmark. Method and commands:
[README.md](README.md).

## Environment

| | |
|---|---|
| Machine | Intel i7-9750H, 5 cores / 10 threads, 11 GB, WSL2 (kernel 6.18.33), shared with Redis, Postgres and the load generator |
| Python | CPython 3.12.3; free threaded rows CPython 3.14.5t |
| Redis | 7.0.15, no persistence |
| PostgreSQL | 16.15 |
| cauli commit | c899bce; memory and web re-run at 8db5c91 after two harness fixes (hold task Redis pool, web readiness wait) |
| Framework versions | Celery 5.6.3, taskiq 0.12.4, Dramatiq 2.2.0, arq 0.28.0, Django 6.0.7, FastAPI 0.162.0; all pins in [requirements.txt](requirements.txt), [requirements-arq.txt](requirements-arq.txt) |

Cores are of a 10 thread box. A Redis near 1.0 core would mean the row
measured Redis; none does.

## dispatch

No-op task (one Redis INCR), every framework at its best measured
configuration, 6 paired rounds. `python -m bench dispatch`

| lane | tasks/s | vs cauli async | 95% interval | worker cores | redis cores |
|---|---:|---:|---|---:|---:|
| cauli async, 8 processes | 35,128 | 1.000 | | 7.80 | 0.83 |
| cauli sync, 12 processes | 22,917 | 0.652 | 0.640 to 0.665 | 8.34 | 0.71 |
| Dramatiq, 12 processes x 8 threads | 11,938 | 0.340 | 0.327 to 0.353 | 7.56 | 0.83 |
| taskiq, 8 workers | 10,360 | 0.295 | 0.288 to 0.301 | 7.56 | 0.54 |
| Celery, 8 solo nodes | 4,476 | 0.127 | 0.122 to 0.133 | 6.22 | 0.82 |
| arq | 1,055 | 0.030 | 0.030 to 0.030 | 1.00 | 0.16 |

cauli async is 2.9x Dramatiq, 3.4x taskiq and 7.8x Celery here. Every row
except arq uses most of the box, so this is throughput per box, not a
framework ceiling.

## cpu

`cpu_burn` busy loop, cauli `kind="cpu"` against Celery prefork, both at 6
processes, 6 paired rounds. `python -m bench cpu`

| task size | cauli tasks/s | Celery tasks/s | Celery vs cauli | 95% interval |
|---|---:|---:|---:|---|
| 0.5 ms | 7,597 | 856 | 0.113 | 0.112 to 0.113 |
| 2 ms | 2,607 | 802 | 0.307 | 0.303 to 0.312 |
| 10 ms | 582 | 545 | 0.936 | 0.930 to 0.941 |
| 50 ms | 119 | 117 | 0.987 | 0.984 to 0.990 |

At 0.5 and 2 ms Celery runs into its single node consumer, which tops out
near 870 tasks/s whatever the process count (the dispatch table scales Celery
by nodes instead), so those two rows measure that ceiling as much as CPU work.
At 10 ms and above both are bound by the cores and differ by a few percent.

## memory

N tasks sleeping at once, PSS summed over the worker's process tree after
every task has reported itself started. `python -m bench memory`

| worker | 100 in flight | 1,000 | 10,000 |
|---|---:|---:|---:|
| cauli async, 1 process | 51.0 MiB | 68.9 MiB | 155.7 MiB |
| cauli async, `-c N` (processes derived) | 110.4 MiB (3) | 456.7 MiB (11) | 557.4 MiB (11) |
| Celery gevent | 50.5 MiB | 75.4 MiB | 324.8 MiB |
| taskiq, 1 worker | 64.7 MiB | 68.5 MiB | 118.3 MiB |
| Celery prefork | 2,345.0 MiB (101) | | |

Parentheses: process count. `-c` derives one process per 64 slots up to the
core count, which buys throughput with memory: about 23 MiB per process. For
memory, run one process with `--io-concurrency N`: then cauli is below
Celery gevent at every size, and taskiq is smaller at 10,000.

## chaos

500 tagged tasks, `kill -9` at 160 executed, fresh worker, count lost and
duplicated tags. `python -m bench chaos`

| lane | lost | duplicated | recovered |
|---|---:|---:|---|
| cauli | 0 | 0 | 29.9 s |
| Celery `acks_late`, `visibility_timeout` 5 | 0 | 0 | 101.1 s |
| Celery default | 8 | 0 | not within 120 s |
| Dramatiq | 24 | 0 | not within 120 s |
| arq | 60 | 0 | not within 120 s |

One segfaulting task among 10 sleeping ones, observed for 20 s:

| lane | top level process | processes died |
|---|---|---:|
| cauli async, 1 process | exited (SIGSEGV) | 1 |
| cauli async, 3 processes | alive, supervisor restarts the process | 1 |
| cauli, segfault in a `kind="cpu"` task | alive | 1 |
| Celery prefork | alive | 1 |
| Dramatiq | exited (code 1) | 3 |
| arq | exited (SIGSEGV) | 1 |

## web

Per arm: closed loop saturation, 64 clients, no worker running; drain with
the worker; open loop at 500 requests/s with the worker live. 3 reps,
medians. `python -m bench web`

| arm | saturation req/s | e2e p50 ms | e2e p99 ms | lost / duplicated / dead letters |
|---|---:|---:|---:|---|
| Django WSGI, insert only | 1,262 | | | |
| Django WSGI, cauli `delay_on_commit` | 1,087 | 2.2 | 11.5 | 0 / 0 / 0 |
| Django WSGI, cauli async task with the async ORM | 1,072 | 3.0 | 43.8 | 0 / 0 / 0 |
| Django WSGI, Celery `.delay` on commit (producer only) | 870 | | | |
| Django ASGI, insert only | 293 | | | |
| Django ASGI, cauli `adelay` | 284 | 1,700 | 3,544 | 0 / 0 / 0 |
| FastAPI, insert only | 2,133 | | | |
| FastAPI, cauli `adelay` | 1,823 | 4.7 | 31.0 | 0 / 0 / 0 |
| FastAPI, cauli `.delay` inside `async def` | 1,788 | 4.3 | 22.8 | 0 / 0 / 0 |

Enqueue cost per request: Django WSGI 14% of the insert only rate with
cauli, 31% with Celery's producer; FastAPI 15%. Django ASGI tops out near 290
requests/s on Django's side, so the 500 requests/s open loop overloads it by
design and its latency columns describe Django under overload.

## free-threaded

The same worker source built against CPython 3.12 and 3.14t, one process,
6 paired rounds. `python -m bench free-threaded`

| lane | GIL tasks/s | free threaded tasks/s | ratio | 95% interval |
|---|---:|---:|---:|---|
| noop, sync, 10 threads | 2,453 | 10,897 | 4.44 | 4.31 to 4.58 |
| pure Python CPU body, sync, 10 threads | 300 | 1,366 | 4.55 | 4.48 to 4.63 |
| pure Python CPU body, cpu lane, 10 processes | 1,544 | 1,959 | 1.27 | 1.25 to 1.29 |

On 3.14t the sync lane runs CPU bound work on 7.5 cores against 1.2 with the
GIL, without forking; the cpu lane gains only the interpreter's own speedup.
