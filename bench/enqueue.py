"""Enqueue N tasks for one lane, with NO worker running (drain-rate setup
phase -- see RESULTS.md for why). Each lane is (module, attr, enqueue
mechanism, fixed args); add a workload/framework combo here as one LANES
entry, not a new branch of framework-specific code.
"""

import asyncio
import importlib
import sys
import time

# lane_name -> (module, attr, mechanism, args)
# mechanism: "delay" (cauli sync/async, celery, dramatiq via .send aliasing
# below), "kiq" (taskiq, async), "arq" (arq's pool.enqueue_job, async).
LANES = {
    "cauli_sync": ("tasks_cauli_sync", "noop", "delay", ()),
    "cauli_async": ("tasks_cauli_async", "noop", "delay", ()),
    "celery": ("tasks_celery", "noop", "delay", ()),
    "taskiq": ("tasks_taskiq", "noop", "kiq", ()),
    "arq": ("tasks_arq", "noop", "arq", ()),
    "dramatiq": ("tasks_dramatiq", "noop", "send", ()),
    "cauli_cpu": ("tasks_cauli_cpu", "burn", "delay", None),  # args from argv[3:]
    "celery_cpu": ("tasks_celery_cpu", "burn", "delay", None),
    "taskiq_cpu": ("tasks_taskiq_cpu", "burn", "kiq", None),
    "arq_cpu": ("tasks_arq_cpu", "burn", "arq", None),
    "dramatiq_cpu": ("tasks_dramatiq_cpu", "burn", "send", None),
    "cauli_sync_pg": ("tasks_cauli_sync_pg", "insert", "delay", ()),
    "cauli_async_pg": ("tasks_cauli_async_pg", "insert", "delay", ()),
    "celery_pg_prefork": ("tasks_celery_pg_prefork", "insert", "delay", ()),
    "celery_pg_gevent": ("tasks_celery_pg_gevent", "insert", "delay", ()),
    "taskiq_pg": ("tasks_taskiq_pg", "insert", "kiq", ()),
    "arq_pg": ("tasks_arq_pg", "insert", "arq", ()),
    "dramatiq_pg": ("tasks_dramatiq_pg", "insert", "send", ()),
    "celery_hold": ("tasks_celery_hold", "hold", "delay", ()),
    "cauli_async_hold": ("tasks_cauli_async_hold", "hold", "delay", ()),
    "celery_gevent": ("tasks_celery_gevent", "noop", "delay", ()),
    "celery_gevent_hold": ("tasks_celery_gevent", "hold", "delay", ()),
    "celery_threads": ("tasks_celery_threads", "noop", "delay", ()),
    "celery_threads_hold": ("tasks_celery_threads", "hold", "delay", ()),
    "cauli_sync_django": ("tasks_cauli_sync_django", "insert", "delay", ()),
    "celery_django": ("tasks_celery_django", "insert", "delay", ()),
    "cauli_async_sqlalchemy": ("tasks_cauli_async_sqlalchemy", "insert", "delay", ()),
    "taskiq_sqlalchemy": ("tasks_taskiq_sqlalchemy", "insert", "kiq", ()),
}


def main():
    lane = sys.argv[1]
    n = int(sys.argv[2])
    extra_args = tuple(_coerce(a) for a in sys.argv[3:])

    module_name, attr_name, mechanism, fixed_args = LANES[lane]
    args = extra_args if fixed_args is None else fixed_args

    t0 = time.perf_counter()

    if mechanism == "delay":
        mod = importlib.import_module(module_name)
        fn = getattr(mod, attr_name)
        for _ in range(n):
            fn.delay(*args)
    elif mechanism == "send":
        mod = importlib.import_module(module_name)
        fn = getattr(mod, attr_name)
        for _ in range(n):
            fn.send(*args)
    elif mechanism == "kiq":
        mod = importlib.import_module(module_name)
        broker = mod.broker
        fn = getattr(mod, attr_name)

        async def run():
            await broker.startup()
            for _ in range(n):
                await fn.kiq(*args)
            await broker.shutdown()

        asyncio.run(run())
    elif mechanism == "arq":
        from arq.connections import create_pool

        mod = importlib.import_module(module_name)
        redis_settings = mod.redis_settings

        async def run():
            pool = await create_pool(redis_settings)
            for _ in range(n):
                await pool.enqueue_job(attr_name, *args)
            await pool.aclose()

        asyncio.run(run())
    else:
        raise SystemExit(f"unknown mechanism {mechanism!r}")

    dt = time.perf_counter() - t0
    print(f"enqueued {n} via {lane} in {dt:.2f}s ({n / dt:.1f}/s)")


def _coerce(raw):
    try:
        return float(raw) if "." in raw else int(raw)
    except ValueError:
        return raw


if __name__ == "__main__":
    main()
