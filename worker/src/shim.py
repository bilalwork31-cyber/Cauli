"""cauli worker embedded shim.

Loaded once by the Rust worker via PyModule::from_code. Owns ALL Python-side
complexity so the pyo3 surface stays "call function, pass strings, get strings".

Contract with Rust:
  load_app(app_spec, extra_paths_json) -> app config JSON (startup only)
  run_sync(name, args, kwargs, soft_timeout_ms) -> outcome dict
  start_loops(n)                      -> spawn N daemon threads running asyncio loops
  set_callback(cb)                    -> register Rust completion callback cb(token, outcome)
  submit_async(token, name, args, kwargs, timeout_s) -> schedules coroutine;
      completion is push-style via the registered callback (no polling).

Task arguments and outcomes cross as REAL PYTHON OBJECTS, not JSON strings.
Rust converts in both directions (worker/src/pyjson.rs). This module therefore
contains no per-task JSON codec at all: it used to carry its own copy of
cauli._codec (a validator, a stdlib/msgspec switch, and an encoder), and every
task paid two `json.loads` plus a validate-and-encode pass **while holding the
GIL** that all in-process tasks share. `load_app`'s config is the one
remaining JSON payload, and it crosses exactly once at startup.

Outcome dict shapes:
  {"ok": True,  "result": <any JSON-representable value>}
  {"ok": False, "retry": True, "countdown": <float|None>, "error": {...}}
  {"ok": False, "retryable": <bool>, "error": {"type": ..., "message": ..., "traceback": ...}}

A result that cannot be represented as JSON is rejected on the Rust side and
becomes a non-retryable SerializationError, the same classification the
deleted encoder produced.
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
# One submission queue per loop, plus its lock. Dispatch appends here and only
# wakes the loop when the queue was empty, so a burst of N tasks costs one
# loop wakeup instead of N. See submit_async for the measured reason.
_pending = []
_pending_locks = []
_callback = None  # Rust completion callback: cb(token:int, outcome:dict)


class AsyncQueueFull(RuntimeError):
    """MEM-5: raised by submit_async when a loop's pending list is already at
    _PENDING_CAP. A distinct type, not a bare RuntimeError, so pyrt.rs can
    count these rejections by checking the exception itself rather than
    parsing message text."""


# MEM-5: hard cap on each loop's pending list. A blocking call inside an
# async task (a synchronous HTTP request, time.sleep, a blocking database
# driver) starves that loop's own callback processing forever; _drain then
# never runs again and this list would otherwise grow without bound, keeping
# real args and kwargs objects alive for the rest of the process lifetime.
# MEM-1 already keeps the Rust side bookkeeping (pyrt.rs) from leaking on its
# own backstop timer, which is exactly why that fix hides this one: the
# pending_async stat stays flat while this list keeps growing underneath it.
# 4096 is 2x the highest --io-concurrency this codebase has measured (2048,
# see the convoying note in pyrt.rs), so a legitimate burst should never
# reach it; past the cap a submission fails fast instead of piling up
# forever.
_PENDING_CAP = 4096
_cap_warned = []  # per loop: already logged the cap hit warning once

# Per-task lifecycle hooks (PROTOCOL §4.8), duck-read off the app object at
# load_app time. These are references to the app's own LISTS, not copies, so
# hooks registered after startup are still honored. Zero-arg callables; a
# hook that raises is logged and skipped, never failing the task.
_before_hooks = ()
_after_hooks = ()


def _run_hooks(hooks, where):
    for hook in hooks:
        try:
            hook()
        except Exception:
            print(
                "cauli-worker: %s hook %r raised (ignored):" % (where, hook),
                file=sys.stderr,
            )
            traceback.print_exc()


async def _run_hooks_async(hooks, where):
    # Async-path variant: a hook may return an awaitable (e.g. one built on
    # asgiref's sync_to_async so it runs in the same executor thread Django's
    # async ORM uses); it is awaited on the loop thread. Sync hooks behave
    # exactly as on the other paths.
    for hook in hooks:
        try:
            r = hook()
            if r is not None and hasattr(r, "__await__"):
                await r
        except Exception:
            print(
                "cauli-worker: %s hook %r raised (ignored):" % (where, hook),
                file=sys.stderr,
            )
            traceback.print_exc()


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

    # The module body above may have failed to import cauli (see the try
    # block at the top of this file): it runs before this function has set
    # up sys.path and VIRTUAL_ENV, so a source built worker with no venv
    # link finds nothing there yet. The app module we just imported pulls
    # the real cauli in now that the paths exist, so rebind onto it when
    # present, or a task's own except SoftTimeLimitExceeded clause compares
    # against a different class object and never matches.
    global SoftTimeLimitExceeded
    real_cauli = sys.modules.get("cauli")
    if real_cauli is not None and hasattr(real_cauli, "SoftTimeLimitExceeded"):
        SoftTimeLimitExceeded = real_cauli.SoftTimeLimitExceeded

    # Lifecycle hooks (PROTOCOL §4.8), by getattr like everything else here:
    # keep the app's list objects so post-startup registrations are seen.
    global _before_hooks, _after_hooks
    _before_hooks = getattr(app, "_before_task_hooks", ())
    _after_hooks = getattr(app, "_after_task_hooks", ())
    # Process-init hooks run once in this (embedded) interpreter, before any
    # task executes — the worker process is itself an execution context.
    _run_hooks(getattr(app, "_process_init_hooks", ()), "process_init")

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

    # PROTOCOL §9.2 queue TTL: {queue: seconds}, key "*" is the fallback for
    # queues with no explicit entry. Duck-read like every other app attribute
    # so an app object predating this feature still loads.
    queue_ttl_out = {}
    try:
        for qname, seconds in dict(getattr(app, "queue_ttl", None) or {}).items():
            value = float(seconds)
            if value > 0:
                queue_ttl_out[str(qname)] = value
    except (TypeError, ValueError):
        # Malformed config must not stop the worker from starting; it just
        # means no TTL is enforced (the client-side `expires_at` stamp still
        # applies to anything enqueued by a client that could read it).
        queue_ttl_out = {}

    return json.dumps(
        {
            "redis_url": str(getattr(app, "redis_url", "redis://localhost:6379/0")),
            "default_queue": str(getattr(app, "default_queue", "default")),
            "result_ttl": int(getattr(app, "result_ttl", 3600)),
            "idemp_ttl": int(getattr(app, "idemp_ttl", 86400)),
            "queue_ttl": queue_ttl_out,
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
    #
    # The injection happens INSIDE the lock, not after it. Releasing the lock
    # between the compare and the injection reopens exactly the hole the
    # generation counter exists to close: the pool thread reaches its disarm
    # (bump generation, clear pending exception) in that gap, and this
    # injection then lands on a thread that has already moved on -- so the
    # NEXT task run on it dies of a SoftTimeLimitExceeded it never armed.
    # Under the GIL that gap needed an eval-loop thread switch to land in and
    # was merely improbable; on a free-threaded build the two threads run at
    # once and nothing makes it improbable any more. Holding the lock across
    # both makes check-and-arm atomic against bump-and-disarm.
    with _gen_lock:
        if _thread_gen.get(tid, 0) != gen:
            return
        _set_async_exc(ctypes.c_ulong(tid), ctypes.py_object(SoftTimeLimitExceeded))


def run_sync(name, args, kwargs, soft_timeout_ms):
    """Run one sync task. `args`/`kwargs` arrive as real Python objects and the
    outcome dict is returned as a real Python object.

    No JSON is produced or parsed anywhere on this path. Rust converts in both
    directions (src/pyjson.rs), because every instruction here holds the GIL
    that all in-process tasks share -- the two `json.loads` calls and the
    validate-plus-encode that used to live here were charged directly against
    total io throughput.
    """
    try:
        return _run_sync_inner(name, args, kwargs, soft_timeout_ms)
    except BaseException as e:  # late soft-timeout injection race, anything else
        try:
            return _finish_exc(e)
        except Exception:
            return {
                "ok": False,
                "retryable": True,
                "error": {
                    "type": "WorkerShimError",
                    "message": "shim failure",
                    "traceback": "",
                },
            }


def _run_sync_inner(name, args, kwargs, soft_timeout_ms):
    td = _registry.get(name)
    if td is None:
        return {
            "ok": False,
            "retryable": False,
            "error": {
                "type": "UnregisteredTask",
                "message": "unknown task %s" % (name,),
                "traceback": "",
            },
        }
    fn = getattr(td, "fn")

    # Before hooks (PROTOCOL §4.8) run on THIS pool thread before the soft
    # timeout is armed: hook time is not charged against the soft budget and
    # an injection cannot land inside a hook. After hooks run after the
    # disarm below, on every outcome path.
    _run_hooks(_before_hooks, "before_task")
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
        _run_hooks(_after_hooks, "after_task")
    return out


# --------------------------------------------------------------------------
# async execution (dedicated daemon threads running asyncio loops; completion
# is pushed to Rust via the registered callback -> mpsc channel, no polling)
# --------------------------------------------------------------------------


def _loop_main(loop):
    asyncio.set_event_loop(loop)
    loop.run_forever()


def _new_loop():
    """Build one event loop, preferring uvloop when it is importable.

    uvloop is a libuv-backed drop in replacement whose timer heap and
    callback scheduling are several times cheaper than the stock loop's;
    on this worker the loop machinery was measured at 60-100us of a 220us
    per-task budget, so the loop implementation is a first order cost, not
    a taste choice. Everything this shim does with a loop --
    call_soon_threadsafe, create_task, run_forever, wait_for -- is in the
    subset uvloop implements.

    CAULI_LOOP=asyncio forces the stock loop (the A/B control and the
    escape hatch); CAULI_LOOP=uvloop makes uvloop mandatory, failing
    loudly at startup instead of silently benchmarking the wrong loop.
    Unset means: use uvloop when available, else stock.
    """
    choice = os.environ.get("CAULI_LOOP", "").strip().lower()
    if choice not in ("", "asyncio", "uvloop"):
        raise RuntimeError(
            "CAULI_LOOP must be 'uvloop', 'asyncio' or unset, got %r" % (choice,)
        )
    if choice == "asyncio":
        return asyncio.new_event_loop(), "asyncio"
    try:
        import uvloop
    except ImportError:
        if choice == "uvloop":
            raise RuntimeError(
                "CAULI_LOOP=uvloop but uvloop is not importable in this "
                "environment; install it or unset CAULI_LOOP"
            ) from None
        return asyncio.new_event_loop(), "asyncio"
    return uvloop.new_event_loop(), "uvloop"


def start_loops(n):
    n = max(1, int(n))
    impl = None
    with _loops_lock:
        for i in range(n):
            loop, impl = _new_loop()
            t = threading.Thread(
                target=_loop_main, args=(loop,), name="cauli-aio-%d" % i, daemon=True
            )
            t.start()
            _loops.append(loop)
            _pending.append([])
            _pending_locks.append(threading.Lock())
            _cap_warned.append(False)
    if impl is not None:
        # One line, stderr: lands in the worker's log so a benchmark or an
        # operator can see WHICH loop ran without introspecting the process.
        sys.stderr.write("cauli: %d asyncio loop(s), impl=%s\n" % (n, impl))
    return len(_loops)


def set_callback(cb):
    global _callback
    _callback = cb


def _drain(idx, loop):
    """Turn one loop's queued submissions into Tasks. Runs ON that loop."""
    queue = _pending[idx]
    with _pending_locks[idx]:
        batch = queue[:]
        del queue[:]
    for token, name, args, kwargs, timeout_s in batch:
        # _arun is defensive (it converts every exception into an outcome and
        # always invokes the callback), so a bare Task needs no result handle.
        loop.create_task(_arun(token, name, args, kwargs, timeout_s))


def submit_async(token, name, args, kwargs, timeout_s):
    """Schedule one async task. `args`/`kwargs` are already Python objects
    (converted in Rust); nothing on this path touches JSON.

    Submissions are queued per loop and the loop is woken only when its queue
    was empty. asyncio.run_coroutine_threadsafe wakes the loop thread for
    every single task, and that wakeup -- not the Task machinery -- is the
    dominant cost of the async path: measured on this codebase's dispatch
    shape, 46.2 us/task submitting one at a time against 4.4 us/task when the
    queue is drained per wakeup (20k tasks, trivial body, one loop thread).
    Ordering within a loop is preserved; a task submitted while a drain is in
    flight simply schedules the next drain.

    Raises AsyncQueueFull if that loop's queue is already at _PENDING_CAP.
    """
    global _rr
    with _loops_lock:
        count = len(_loops)
        if not count:
            raise RuntimeError("start_loops() was not called")
        idx = _rr % count
        _rr += 1
        loop = _loops[idx]

    with _pending_locks[idx]:
        if len(_pending[idx]) >= _PENDING_CAP:
            if not _cap_warned[idx]:
                _cap_warned[idx] = True
                sys.stderr.write(
                    "cauli: async loop %d queue hit its cap of %d pending "
                    "submissions; rejecting new ones until it drains. Likely "
                    "cause: a blocking call inside an async task body (a "
                    "synchronous HTTP request, time.sleep, a blocking "
                    "database driver) in place of its non blocking "
                    "equivalent, which starves this loop of its own event "
                    "loop turns.\n" % (idx, _PENDING_CAP)
                )
            raise AsyncQueueFull(
                "cauli: async loop %d submission queue is full (cap=%d); "
                "rejecting so the task can be retried instead of queued "
                "forever" % (idx, _PENDING_CAP)
            )
        _pending[idx].append((token, name, args, kwargs, timeout_s))
        wake = len(_pending[idx]) == 1
    if wake:
        loop.call_soon_threadsafe(_drain, idx, loop)


async def _arun(token, name, args, kwargs, timeout_s):
    try:
        td = _registry.get(name)
        if td is None:
            out = {
                "ok": False,
                "retryable": False,
                "error": {
                    "type": "UnregisteredTask",
                    "message": "unknown task %s" % (name,),
                    "traceback": "",
                },
            }
        else:
            fn = getattr(td, "fn")
            # Before/after hooks (PROTOCOL §4.8) on the loop thread, outside
            # the wait_for window (hook time is not charged against the task
            # timeout). Awaitable-returning hooks are awaited.
            # Guarded, not called unconditionally: an await on a coroutine
            # that immediately returns still costs ~3.5us of GIL time each
            # way, which is real money against a small task body.
            if _before_hooks:
                await _run_hooks_async(_before_hooks, "before_task")
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
            finally:
                if _after_hooks:
                    await _run_hooks_async(_after_hooks, "after_task")
    except BaseException as e:  # defensive: never lose a completion
        out = {"ok": False, "retryable": True, "error": _error_dict(e)}

    cb = _callback
    if cb is not None:
        try:
            # The outcome dict itself, not JSON text: Rust normalizes it in the
            # callback (pyrt::outcome_from_py), which runs right here on this
            # loop thread. A non-serializable result becomes SerializationError
            # there, exactly as the old encode-and-catch did.
            cb(token, out)
        except Exception:
            traceback.print_exc()
