# Benchmark results

One machine, every benchmark; dispatch, cpu, memory and chaos run on both
CPython 3.12 and free threaded 3.14t back to back. Method and commands:
[README.md](README.md).

## Environment

| | |
|---|---|
| Machine | Intel i7-9750H, 5 cores / 10 threads, 11 GB, WSL2 (kernel 6.18.33), shared with Redis, Postgres and the load generator |
| Python | CPython 3.12.3 and 3.14.5t (free threaded), each with a worker built against it |
| Redis | 7.0.15, no persistence |
| PostgreSQL | 16.15 |
| cauli commit | 72ab716 for dispatch, cpu, memory and chaos (one session, both builds); web and free-threaded from an earlier session at 8db5c91 and c899bce |
| Framework versions | Celery 5.6.3, taskiq 0.12.4, Dramatiq 2.2.0, arq 0.28.0, Django 6.0.7, FastAPI 0.162.0; all pins in [requirements.txt](requirements.txt), [requirements-arq.txt](requirements-arq.txt) |

Cores are of a 10 thread box. A Redis near 1.0 core would mean the row
measured Redis; none does. On 3.14t, Dramatiq, gevent and arq (through
hiredis) load a C extension that has not declared free threading support,
so their 3.14t rows run with the GIL re-enabled; cauli, Celery prefork and
solo, and taskiq keep it off.

## dispatch

No-op task (one Redis INCR), every framework at its best measured
configuration, 6 paired rounds per build. `python -m bench dispatch`

| lane | 3.12 tasks/s | 3.14t tasks/s | 3.12 vs cauli async, 95% interval |
|---|---:|---:|---|
| cauli async, 8 processes | 31,712 | 32,487 | |
| cauli sync, 12 processes | 19,340 | 21,301 | 0.614 (0.542 to 0.686) |
| Dramatiq, 12 processes x 8 threads | 11,407 | 10,494 | 0.369 (0.295 to 0.444) |
| taskiq, 8 workers | 9,736 | 9,394 | 0.315 (0.256 to 0.374) |
| Celery, 8 solo nodes | 4,462 | 4,248 | 0.145 (0.113 to 0.177) |
| arq | 1,030 | 1,022 | 0.033 (0.027 to 0.040) |

cauli async is 2.8x Dramatiq, 3.3x taskiq and 7.1x Celery on 3.12. Every
row except arq uses most of the box (6.2 to 7.9 worker cores), so this is
throughput per box, not a framework ceiling. An earlier session on the same
box measured cauli async at 35,128/s; the box varies by about 10% between
sessions, which is why every comparison is a paired ratio.

## cpu

`cpu_burn` busy loop, cauli `kind="cpu"` against Celery prefork, both at 6
processes, 6 paired rounds per build. `python -m bench cpu`

| task size | cauli 3.12 | Celery 3.12 | cauli 3.14t | Celery 3.14t | Celery vs cauli (3.12) |
|---|---:|---:|---:|---:|---|
| 0.5 ms | 7,582 | 853 | 7,520 | 852 | 0.113 (0.112 to 0.113) |
| 2 ms | 2,603 | 802 | 2,599 | 797 | 0.308 (0.306 to 0.311) |
| 10 ms | 582 | 545 | 581 | 543 | 0.937 (0.932 to 0.942) |
| 50 ms | 119 | 118 | 119 | 117 | 0.989 (0.985 to 0.994) |

At 0.5 and 2 ms Celery runs into its single node consumer, which tops out
near 870 tasks/s whatever the process count (the dispatch table scales Celery
by nodes instead), so those two rows measure that ceiling as much as CPU work.
At 10 ms and above both are bound by the cores and differ by a few percent.
Both builds give the same numbers: forked processes do not need the GIL gone.

## memory

N tasks sleeping at once, PSS summed over the worker's process tree after
every task has reported itself started. `python -m bench memory`

| worker | 100 in flight | 1,000 | 10,000 |
|---|---:|---:|---:|
| cauli async, 1 process, 3.12 | 59.3 MiB | 65.8 MiB | 151.8 MiB |
| cauli async, 1 process, 3.14t | 74.7 MiB | 83.5 MiB | 165.9 MiB |
| cauli async, `-c N`, 3.12 | 109.7 MiB (3) | 457.8 MiB (11) | 548.7 MiB (11) |
| Celery gevent, 3.12 | 51.5 MiB | 76.4 MiB | 325.8 MiB |
| Celery gevent, 3.14t | 76.0 MiB | 101.4 MiB | 383.3 MiB |
| taskiq, 1 worker, 3.12 | 65.9 MiB (2) | 69.7 MiB (2) | 120.4 MiB (2) |
| taskiq, 1 worker, 3.14t | 145.5 MiB (4) | 148.8 MiB (4) | 190.1 MiB (4) |
| Celery prefork, 3.12 | 2,356.2 MiB (101) | | |
| Celery prefork, 3.14t | 2,934.8 MiB (101) | | |

Parentheses: process count. `-c` derives one process per 64 slots up to the
core count, which buys throughput with memory: about 23 MiB per process. For
memory, run one process with `--io-concurrency N`: then cauli is below
Celery gevent at every size on both builds, and taskiq is smaller at 10,000
on 3.12. Every interpreter costs more memory free threaded.

## chaos

500 tagged tasks, `kill -9` at 160 executed, fresh worker, count lost and
duplicated tags. `python -m bench chaos`

| lane | lost, 3.12 / 3.14t | duplicated, 3.12 / 3.14t | recovered |
|---|---|---|---|
| cauli | 0 / 0 | 1 / 0 | 29.8 s |
| Celery `acks_late`, `visibility_timeout` 5 | 0 / 0 | 0 / 0 | 101.1 s |
| Celery default | 8 / 8 | 0 / 0 | not within 120 s |
| Dramatiq | 24 / 24 | 0 / 0 | not within 120 s |
| arq | 43 / 61 | 0 / 0 | not within 120 s |

cauli's delivery is at least once: the one duplicate is a task that finished
inside the 2 ms ack window before the kill, then ran again on redelivery.

One segfaulting task among 10 sleeping ones, observed for 20 s, same on both
builds:

| lane | top level process | processes died |
|---|---|---:|
| cauli async, 1 process | exited (SIGSEGV) | 1 |
| cauli async, 3 processes | alive, supervisor restarts the process | 1 |
| cauli, segfault in a `kind="cpu"` task | alive | 1 |
| Celery prefork | alive | 1 |
| Dramatiq | exited (code 1) | 3 (3.12), 5 (3.14t) |
| arq | exited (SIGSEGV) | 1 |

## Process count on 3.14t

Framework only noop tasks, one drain each, CPU per task from `/proc`.

| setup | 3.12 tasks/s | 3.14t tasks/s |
|---|---:|---:|
| async, 1 process | 121,845 | 128,223 |
| async, 8 processes | 163,830 | 160,335 |
| async, 1 process, `--io-loops 4` | 95,071 | 33,054 |
| sync, 1 process, 64 threads | 114,446 | 37,045 |
| sync, 8 processes, 8 threads each | 173,931 | 158,607 |

Free threading does not remove the need for processes. One event loop per
process stays the async ceiling, and extra loops are slower on both builds.
The sync pool hands each job to a GIL holder; without a GIL all 64 threads
contend for it, and one 3.14t process runs noop sync tasks at a third of
the 3.12 rate. With real CPU work in the task the free threaded sync lane is
4.6x the 3.12 one (free-threaded section below).

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
