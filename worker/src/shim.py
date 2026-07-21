"""cauli worker embedded shim.

Loaded once by the Rust worker via PyModule::from_code. Owns ALL Python-side
complexity so the pyo3 surface stays "call function, pass strings, get strings".

Contract with Rust (all payloads are JSON strings):
  load_app(app_spec, extra_paths_json) -> app config JSON (see below)
  run_sync(name, args_json, kwargs_json, soft_timeout_ms) -> outcome JSON
  start_loops(n)                      -> spawn N daemon threads running asyncio loops
  set_callback(cb)                    -> register Rust completion callback cb(token, outcome_json)
  submit_async(token, name, args_json, kwargs_json, timeout_s) -> schedules coroutine;
      completion is push-style via the registered callback (no polling).

Outcome JSON shapes:
  {"ok": true,  "result": <json>}
  {"ok": false, "retry": true, "countdown": <float|null>, "error": {...}}
  {"ok": false, "retryable": <bool>, "error": {"type": ..., "message": ..., "traceback": ...}}
"""

import asyncio
import ctypes
import glob
import heapq
import importlib
import json
import os
import sys
import threading
import time
import traceback

_MAX_TB = 8192

try:  # pragma: no cover - only when the real cauli package is importable
    from cauli import SoftTimeLimitExceeded  # type: ignore
except Exception:

    class SoftTimeLimitExceeded(Exception):
        """Soft time limit exceeded (local stand-in for cauli.SoftTimeLimitExceeded).

        Derives from Exception (not BaseException) to match the real
        cauli.SoftTimeLimitExceeded (audit L5): a task's `except Exception`
        must catch this in the fallback case too.
        """


_registry = {}  # task name -> duck-typed TaskDef object
_loops = []  # asyncio loops, one per dedicated daemon thread
_loops_lock = threading.Lock()
_rr = 0
_callback = None  # Rust completion callback: cb(token:int, outcome_json:str)

_set_async_exc = ctypes.pythonapi.PyThreadState_SetAsyncExc


def _tb_of(exc):
    try:
        tb = "".join(traceback.format_exception(type(exc), exc, exc.__traceback__))
    except Exception:
        tb = ""
    return tb[-_MAX_TB:]


def _error_dict(exc):
    return {"type": type(exc).__name__, "message": str(exc), "traceback": _tb_of(exc)}


def _is_retry(exc):
    # Duck-typed recognition per protocol: class named "Retry" with .countdown.
    return type(exc).__name__ == "Retry" and hasattr(exc, "countdown")


def _finish_value(rv):
    return {"ok": True, "result": rv}


def _outcome_json(out):
    """Serialize an outcome dict exactly once.

    Previously `_finish_value` did a throwaway `json.dumps(rv)` just to probe
    serializability, then the caller dumped the whole outcome again -- every
    successful result was JSON-encoded twice. Encoding once here and catching
    the failure gives the same SerializationError outcome (section 8) for one
    serialization attempt instead of two.
    """
    try:
        return json.dumps(out)
    except (TypeError, ValueError) as e:
        return json.dumps(
            {
                "ok": False,
                "retryable": False,
                "error": {
                    "type": "SerializationError",
                    "message": "task return value is not JSON serializable: %s" % (e,),
                    "traceback": "",
                },
            }
        )


def _finish_exc(exc):
    if _is_retry(exc):
        cd = getattr(exc, "countdown", None)
        if cd is not None:
            try:
                cd = float(cd)
            except Exception:
                cd = None
        return {"ok": False, "retry": True, "countdown": cd, "error": _error_dict(exc)}
    return {"ok": False, "retryable": True, "error": _error_dict(exc)}


def load_app(app_spec, extra_paths_json):
    """Import module:attr, read the app config + task registry by duck typing."""
    extra = json.loads(extra_paths_json or "[]")
    for p in [os.getcwd()] + list(extra):
        if p and p not in sys.path:
            sys.path.insert(0, p)
    venv = os.environ.get("VIRTUAL_ENV")
    if venv:
        import site

        for sp in glob.glob(os.path.join(venv, "lib", "python*", "site-packages")):
            # addsitedir (not sys.path.append) so .pth hooks run — editable
            # installs (pip install -e) are invisible without them
            site.addsitedir(sp)

    module_name, sep, attr = app_spec.partition(":")
    if not sep or not attr:
        attr = "app"
    mod = importlib.import_module(module_name)
    app = getattr(mod, attr)

    tasks_out = {}
    for name, td in dict(getattr(app, "_tasks")).items():
        name = str(name)
        _registry[name] = td
        soft = getattr(td, "soft_timeout_ms", None)
        tasks_out[name] = {
            "kind": str(getattr(td, "kind", "io") or "io"),
            "is_async": bool(getattr(td, "is_async", False)),
            "queue": getattr(td, "queue", None),
            "max_retries": int(getattr(td, "max_retries", 3)),
            "timeout_ms": int(getattr(td, "timeout_ms", 300000)),
            "soft_timeout_ms": None if soft is None else int(soft),
            "backoff_base_ms": int(getattr(td, "backoff_base_ms", 500)),
            "backoff_factor": float(getattr(td, "backoff_factor", 2.0)),
            "backoff_max_ms": int(getattr(td, "backoff_max_ms", 60000)),
            "jitter": bool(getattr(td, "jitter", True)),
            "store_result": bool(getattr(td, "store_result", True)),
        }

    return json.dumps(
        {
            "redis_url": str(getattr(app, "redis_url", "redis://localhost:6379/0")),
            "default_queue": str(getattr(app, "default_queue", "default")),
            "result_ttl": int(getattr(app, "result_ttl", 3600)),
            "idemp_ttl": int(getattr(app, "idemp_ttl", 86400)),
            "tasks": tasks_out,
        }
    )


# --------------------------------------------------------------------------
# sync execution (called on the Rust-owned sync io thread pool, GIL held on
# entry; CPython releases the GIL itself during blocking I/O in the task)
# --------------------------------------------------------------------------

_gen_lock = threading.Lock()
_thread_gen = {}  # tid -> generation fencing stale soft-timeout injections

# MEM-4: one shared watchdog thread services every soft_timeout deadline via a
# min-heap of (deadline, tid, gen), instead of spawning a dedicated
# threading.Timer (and its own OS thread) per sync task call. At hot-path
# rates (hundreds of tasks/s with soft_timeout set) that was thousands of
# thread creations per second; a single background thread sleeping until the
# next deadline scales to however many tasks are in flight with no per-call
# thread spawn/teardown cost.
_watchdog_heap = []  # heap of (deadline_monotonic, tid, gen)
_watchdog_cond = threading.Condition()
_watchdog_started = False


def _watchdog_loop():
    while True:
        with _watchdog_cond:
            while not _watchdog_heap:
                _watchdog_cond.wait()
            deadline, tid, gen = _watchdog_heap[0]
            remaining = deadline - time.monotonic()
            if remaining > 0:
                _watchdog_cond.wait(timeout=remaining)
                continue
            heapq.heappop(_watchdog_heap)
        _inject_soft(tid, gen)


def _ensure_watchdog_started():
    global _watchdog_started
    if _watchdog_started:
        return
    with _watchdog_cond:
        if not _watchdog_started:
            _watchdog_started = True
            t = threading.Thread(
                target=_watchdog_loop, name="cauli-soft-timeout-watchdog", daemon=True
            )
            t.start()


def _schedule_soft_timeout(tid, gen, soft_timeout_ms):
    _ensure_watchdog_started()
    deadline = time.monotonic() + soft_timeout_ms / 1000.0
    with _watchdog_cond:
        heapq.heappush(_watchdog_heap, (deadline, tid, gen))
        _watchdog_cond.notify()


def _inject_soft(tid, gen):
    # M3 stale-timer guard: a deadline scheduled for a PREVIOUS task on this
    # pool thread fires (from the watchdog heap) after that task already
    # finished. Only inject if this thread is still on the generation that
    # armed it -- otherwise the exception would land inside whatever task the
    # thread has since moved on to. The watchdog does not (and structurally
    # cannot cheaply) remove heap entries early, so EVERY scheduled deadline
    # reaches this check; the generation compare is what makes that safe.
    with _gen_lock:
        if _thread_gen.get(tid, 0) != gen:
            return
    _set_async_exc(ctypes.c_ulong(tid), ctypes.py_object(SoftTimeLimitExceeded))


def run_sync(name, args_json, kwargs_json, soft_timeout_ms):
    try:
        return _outcome_json(
            _run_sync_inner(name, args_json, kwargs_json, soft_timeout_ms)
        )
    except BaseException as e:  # late soft-timeout injection race, anything else
        try:
            return _outcome_json(_finish_exc(e))
        except Exception:
            return '{"ok": false, "retryable": true, "error": {"type": "WorkerShimError", "message": "shim failure", "traceback": ""}}'


def _run_sync_inner(name, args_json, kwargs_json, soft_timeout_ms):
    td = _registry.get(name)
    if td is None:
        return {
            "ok": False,
            "retryable": False,
            "error": {
                "type": "Unregistered",
                "message": "unknown task %s" % (name,),
                "traceback": "",
            },
        }
    fn = getattr(td, "fn")
    args = json.loads(args_json)
    kwargs = json.loads(kwargs_json)

    tid = threading.get_ident()
    gen = _thread_gen.get(tid, 0)
    if soft_timeout_ms is not None and soft_timeout_ms > 0:
        _schedule_soft_timeout(tid, gen, soft_timeout_ms)
    try:
        rv = fn(*args, **kwargs)
        out = _finish_value(rv)
    except BaseException as e:
        out = _finish_exc(e)
    finally:
        # Bump the generation (M3): fences off a watchdog deadline for THIS
        # invocation (already scheduled above, if any) so it cannot land
        # inside whatever task this thread picks up next. The residual window
        # where the deadline fires after fn() returns but before this finally
        # block runs -- flipping a successful execution into a
        # SoftTimeLimitExceeded failure -- is inherent to
        # PyThreadState_SetAsyncExc and not fixed by the generation counter
        # (it is the SAME generation); documented in PROTOCOL.md §4.6.
        with _gen_lock:
            _thread_gen[tid] = gen + 1
        _set_async_exc(ctypes.c_ulong(tid), None)
    return out


# --------------------------------------------------------------------------
# async execution (dedicated daemon threads running asyncio loops; completion
# is pushed to Rust via the registered callback -> mpsc channel, no polling)
# --------------------------------------------------------------------------


def _loop_main(loop):
    asyncio.set_event_loop(loop)
    loop.run_forever()


def start_loops(n):
    n = max(1, int(n))
    with _loops_lock:
        for i in range(n):
            loop = asyncio.new_event_loop()
            t = threading.Thread(
                target=_loop_main, args=(loop,), name="cauli-aio-%d" % i, daemon=True
            )
            t.start()
            _loops.append(loop)
    return len(_loops)


def set_callback(cb):
    global _callback
    _callback = cb


def submit_async(token, name, args_json, kwargs_json, timeout_s):
    global _rr
    with _loops_lock:
        if not _loops:
            raise RuntimeError("start_loops() was not called")
        loop = _loops[_rr % len(_loops)]
        _rr += 1
    asyncio.run_coroutine_threadsafe(
        _arun(token, name, args_json, kwargs_json, timeout_s), loop
    )


async def _arun(token, name, args_json, kwargs_json, timeout_s):
    try:
        td = _registry.get(name)
        if td is None:
            out = {
                "ok": False,
                "retryable": False,
                "error": {
                    "type": "Unregistered",
                    "message": "unknown task %s" % (name,),
                    "traceback": "",
                },
            }
        else:
            fn = getattr(td, "fn")
            args = json.loads(args_json)
            kwargs = json.loads(kwargs_json)
            try:
                rv = await asyncio.wait_for(fn(*args, **kwargs), timeout_s)
                out = _finish_value(rv)
            except (asyncio.TimeoutError, TimeoutError):
                out = {
                    "ok": False,
                    "retryable": True,
                    "error": {
                        "type": "TimeoutError",
                        "message": "task timed out after %.3fs" % (timeout_s,),
                        "traceback": "",
                    },
                }
            except BaseException as e:
                out = _finish_exc(e)
    except BaseException as e:  # defensive: never lose a completion
        out = {"ok": False, "retryable": True, "error": _error_dict(e)}

    cb = _callback
    if cb is not None:
        try:
            cb(token, _outcome_json(out))
        except Exception:
            traceback.print_exc()
