"""rupy worker embedded shim.

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
import importlib
import json
import os
import sys
import threading
import traceback

_MAX_TB = 8192

try:  # pragma: no cover - only when the real rupy package is importable
    from rupy import SoftTimeLimitExceeded  # type: ignore
except Exception:
    class SoftTimeLimitExceeded(BaseException):
        """Soft time limit exceeded (local stand-in for rupy.SoftTimeLimitExceeded)."""


_registry = {}          # task name -> duck-typed TaskDef object
_loops = []             # asyncio loops, one per dedicated daemon thread
_loops_lock = threading.Lock()
_rr = 0
_callback = None        # Rust completion callback: cb(token:int, outcome_json:str)

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
    try:
        json.dumps(rv)
    except (TypeError, ValueError) as e:
        return {
            "ok": False,
            "retryable": False,
            "error": {
                "type": "SerializationError",
                "message": "task return value is not JSON serializable: %s" % (e,),
                "traceback": "",
            },
        }
    return {"ok": True, "result": rv}


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

    return json.dumps({
        "redis_url": str(getattr(app, "redis_url", "redis://localhost:6379/0")),
        "default_queue": str(getattr(app, "default_queue", "default")),
        "result_ttl": int(getattr(app, "result_ttl", 3600)),
        "idemp_ttl": int(getattr(app, "idemp_ttl", 86400)),
        "tasks": tasks_out,
    })


# --------------------------------------------------------------------------
# sync execution (called on the Rust-owned sync io thread pool, GIL held on
# entry; CPython releases the GIL itself during blocking I/O in the task)
# --------------------------------------------------------------------------

def _inject_soft(tid):
    _set_async_exc(ctypes.c_ulong(tid), ctypes.py_object(SoftTimeLimitExceeded))


def run_sync(name, args_json, kwargs_json, soft_timeout_ms):
    try:
        return json.dumps(_run_sync_inner(name, args_json, kwargs_json, soft_timeout_ms))
    except BaseException as e:  # late soft-timeout injection race, anything else
        try:
            return json.dumps(_finish_exc(e))
        except Exception:
            return '{"ok": false, "retryable": true, "error": {"type": "WorkerShimError", "message": "shim failure", "traceback": ""}}'


def _run_sync_inner(name, args_json, kwargs_json, soft_timeout_ms):
    td = _registry.get(name)
    if td is None:
        return {
            "ok": False,
            "retryable": False,
            "error": {"type": "Unregistered", "message": "unknown task %s" % (name,), "traceback": ""},
        }
    fn = getattr(td, "fn")
    args = json.loads(args_json)
    kwargs = json.loads(kwargs_json)

    tid = threading.get_ident()
    timer = None
    if soft_timeout_ms is not None and soft_timeout_ms > 0:
        timer = threading.Timer(soft_timeout_ms / 1000.0, _inject_soft, (tid,))
        timer.daemon = True
        timer.start()
    try:
        rv = fn(*args, **kwargs)
        out = _finish_value(rv)
    except BaseException as e:
        out = _finish_exc(e)
    finally:
        if timer is not None:
            timer.cancel()
            # Clear a pending async exc that raced past cancel() (best effort).
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
            t = threading.Thread(target=_loop_main, args=(loop,),
                                 name="rupy-aio-%d" % i, daemon=True)
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
        _arun(token, name, args_json, kwargs_json, timeout_s), loop)


async def _arun(token, name, args_json, kwargs_json, timeout_s):
    try:
        td = _registry.get(name)
        if td is None:
            out = {
                "ok": False,
                "retryable": False,
                "error": {"type": "Unregistered", "message": "unknown task %s" % (name,), "traceback": ""},
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
            cb(token, json.dumps(out))
        except Exception:
            traceback.print_exc()
