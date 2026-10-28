# Benchmark results

One session, one machine, every benchmark. Method and commands:
[README.md](README.md).

## Environment

| | |
|---|---|
| Machine | |
| Kernel | |
| Python | |
| Redis | |
| PostgreSQL | |
| cauli commit | |
| Framework versions | [requirements.txt](requirements.txt), [requirements-arq.txt](requirements-arq.txt) |

## dispatch

No-op task, every framework at its best measured configuration, paired
rounds. `python -m bench dispatch`

## cpu

`cpu_burn` busy loop at 0.5, 2, 10 and 50 ms, cauli `kind="cpu"` against Celery
prefork, both at 6 processes, paired rounds. `python -m bench cpu`

## memory

N tasks sleeping at once, PSS summed over the worker's process tree after every
task has reported itself started. `python -m bench memory`

## chaos

500 tagged tasks, `kill -9` at 160 executed, fresh worker, count lost and
duplicated tags. Then one segfaulting task among 10 sleeping ones. `python -m bench chaos`

## web

Per arm: closed loop saturation with no worker, drain, open loop at 500
requests/s with the worker live; `processed` per row checks exactly once.
`python -m bench web`

## free-threaded

The same worker source built against CPython 3.12 and 3.14t, one process,
paired rounds. `python -m bench free-threaded`
