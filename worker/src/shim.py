"""cauli worker embedded shim, loaded once by the Rust worker (worker/src/pyrt.rs).

Contract with Rust:
  load_app(app_spec, extra_paths_json) -> app config JSON (startup only)
  run_sync(name, args, kwargs, soft_timeout_ms, meta) -> outcome dict
  start_loops(n)                      -> spawn N daemon threads running asyncio loops
  set_callback(cb)                    -> register Rust completion callback cb(token, outcome)
  submit_async(token, name, args, kwargs, timeout_s, soft_timeout_s, meta)
      -> schedules coroutine; completion is pushed through the callback.
  heartbeat()                         -> per loop (lag_ms, pending, completed)

`meta` is the envelope tuple `(id, retries, max_retries, queue)` behind
`cauli.current_task()`, or None. One tuple, not four scalars, to keep the
per task pyo3 call small. Args arrive decoded with `_decode` and results are
encoded with `_encode`, both msgspec, the client's codec.

Outcome dict shapes:
  {"ok": True,  "result": <any JSON-representable value>}
  {"ok": False, "retry": True, "countdown": <float|None>, "error": {...}}
  {"ok": False, "retryable": <bool>, "error": {"type": ..., "message": ...,
                                               "traceback": ..., "origin": ...}}
"""

import asyncio
import ctypes
import gc
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

_decode = None  # msgspec.json.Decoder().decode, bound by load_app
_encode = None  # msgspec.json.Encoder().encode, bound by load_app

try:  # pragma: no cover - only when the real cauli package is importable
    from cauli import SoftTimeLimitExceeded  # type: ignore
except Exception:

    class SoftTimeLimitExceeded(Exception):
        """Stand-in for cauli.SoftTimeLimitExceeded; an Exception like the real one."""


try:  # pragma: no cover - only when the real cauli package is importable
    from cauli._context import TaskContext as _TaskContext  # type: ignore
    from cauli._context import reset_current_task as _reset_ctx  # type: ignore
    from cauli._context import set_current_task as _set_ctx  # type: ignore
except Exception:
    # No cauli here means no current_task() to serve, so the lanes skip it.
    _TaskContext = None
    _set_ctx = None
    _reset_ctx = None


def _enter_context(name, meta):
    """Install `cauli.current_task()` for this attempt; returns a reset token."""
    if meta is None or _TaskContext is None:
        return None
    task_id, retries, max_retries, queue = meta
    return _set_ctx(_TaskContext(task_id, name, retries, max_retries, queue))


def _leave_context(token):
    """Undo `_enter_context`. Always in a `finally`: pool threads are reused."""
    if token is not None:
        _reset_ctx(token)


_registry = {}  # task name -> duck-typed TaskDef object
_loops = []  # asyncio loops, one per dedicated daemon thread
_loops_lock = threading.Lock()
_rr = 0
# Per loop submission queue: a burst of N tasks costs one loop wakeup, not N.
_pending = []
_pending_locks = []
_deadline_heaps = []  # per loop: _DeadlineHeap, touched only on that loop
_callback = None  # Rust completion callback: cb(token:int, outcome:dict)


class AsyncQueueFull(RuntimeError):
    """A loop's pending list is at _PENDING_CAP. Its own type so pyrt.rs can count it."""


# A blocking call in an async task stops _drain on that loop, and without a
# cap its pending list (holding args and kwargs) grows forever. 4096 is twice
# the highest --io-concurrency measured, so a legitimate burst never hits it.
_PENDING_CAP = 4096
_cap_warned = []  # per loop: already logged the cap hit warning once

# Per loop liveness for the wedge watchdog (worker/src/loops.rs). One stamp
# outstanding per loop, or a stalled loop piles up stamps that all fire later.
_hb_sent = []  # monotonic of the stamp this loop has not run yet, or None
_hb_lag = []  # ms the last stamp took from scheduled to run
_hb_sub = []  # async tasks handed to this loop (submit side writes)
_hb_done = []  # async tasks completed on this loop (only that loop writes)

# Lifecycle hooks (PROTOCOL §4.8): the app's own lists, not copies, so hooks
# registered after startup still run. A hook that raises is logged and skipped.
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
    # A hook may return an awaitable (asgiref's sync_to_async, say).
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


def _error_dict(exc, origin="task"):
    # PROTOCOL §8 `origin`: an injected SoftTimeLimitExceeded left user code,
    # so it is "task"; callers pass "worker" when the shim itself failed.
    return {
        "type": type(exc).__name__,
        "message": str(exc),
        "traceback": _tb_of(exc),
        "origin": origin,
    }


def _is_retry(exc):
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
            # addsitedir, not sys.path.append: editable installs need .pth hooks.
            site.addsitedir(sp)

    # Imported only now: the app's environment is on sys.path from here on.
    global _decode, _encode
    try:
        import msgspec.json
    except ImportError as exc:
        raise ImportError(
            "cauli-worker needs msgspec in the Python environment it runs "
            "(%s): it decodes task arguments and encodes results with it, as "
            "the cauli client does. Install cauli, which depends on it, or "
            "msgspec itself into that environment." % (sys.prefix,)
        ) from exc
    _decode = msgspec.json.Decoder().decode
    _encode = msgspec.json.Encoder().encode

    module_name, sep, attr = app_spec.partition(":")
    if not sep or not attr:
        attr = "app"
    mod = importlib.import_module(module_name)
    app = getattr(mod, attr)

    # The module level cauli import ran before sys.path was set up. Rebind to
    # the real class, or a task's `except SoftTimeLimitExceeded` never matches.
    global SoftTimeLimitExceeded
    real_cauli = sys.modules.get("cauli")
    if real_cauli is not None and hasattr(real_cauli, "SoftTimeLimitExceeded"):
        SoftTimeLimitExceeded = real_cauli.SoftTimeLimitExceeded

    # Same rebind for current_task(). All three names or none: a context that
    # cannot be reset leaks the previous task's id onto a reused pool thread.
    global _TaskContext, _set_ctx, _reset_ctx
    real_ctx = sys.modules.get("cauli._context")
    if real_ctx is not None:
        try:
            ctx_cls = real_ctx.TaskContext
            setter = real_ctx.set_current_task
            resetter = real_ctx.reset_current_task
        except AttributeError:
            ctx_cls = setter = resetter = None
        _TaskContext, _set_ctx, _reset_ctx = ctx_cls, setter, resetter

    global _before_hooks, _after_hooks
    _before_hooks = getattr(app, "_before_task_hooks", ())
    _after_hooks = getattr(app, "_after_task_hooks", ())
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

    # PROTOCOL §9.2 queue TTL: {queue: seconds}, "*" is the fallback.
    queue_ttl_out = {}
    try:
        for qname, seconds in dict(getattr(app, "queue_ttl", None) or {}).items():
            value = float(seconds)
            if value > 0:
                queue_ttl_out[str(qname)] = value
    except (TypeError, ValueError):
        # Malformed config must not stop startup; the client's `expires_at` still applies.
        queue_ttl_out = {}

    # Freeze the app image after import so gen2 collections stop rescanning
    # it under the GIL every io lane shares. Must stay after the registry build.
    gc.collect()
    gc.freeze()

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


def cpu_child_python():
    """The interpreter for cpu children: one that loads this same libpython.

    An embedded `sys.executable` comes from PATH and can be another CPython,
    so it (or `$VIRTUAL_ENV`) is used only when its binary or pyvenv.cfg
    `home` is this runtime's BINDIR; otherwise BINDIR's own interpreter.
    """
    import sysconfig

    home = os.path.realpath(sysconfig.get_config_var("BINDIR") or "")
    own_name = f"python{sys.version_info.major}.{sys.version_info.minor}{sys.abiflags}"

    def venv_home(exe):
        cfg = os.path.join(os.path.dirname(os.path.dirname(exe)), "pyvenv.cfg")
        if not os.path.isfile(cfg):
            return None
        with open(cfg, encoding="utf-8") as f:
            for line in f:
                key, _, value = line.partition("=")
                if key.strip() == "home":
                    return os.path.realpath(value.strip())
        return None

    def belongs(exe):
        if not exe or not os.path.isfile(exe):
            return False
        real = os.path.realpath(exe)
        if os.path.basename(real) != own_name:
            return False
        base_dir = venv_home(exe) or os.path.dirname(real)
        return base_dir == home

    venv = os.environ.get("VIRTUAL_ENV")
    for exe in (sys.executable, venv and os.path.join(venv, "bin", "python")):
        if belongs(exe):
            return exe
    own = os.path.join(home, own_name)
    return own if os.path.isfile(own) else (sys.executable or None)


_gen_lock = threading.Lock()
_thread_gen = {}  # tid -> generation fencing stale soft-timeout injections

# One watchdog thread for every sync soft deadline: a threading.Timer per task
# is an OS thread per task.
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
    # Heap entries are never removed early, so every deadline reaches this
    # generation check, or it would hit the thread's next task. Inject inside
    # the lock: free threaded builds can otherwise disarm between check and inject.
    with _gen_lock:
        if _thread_gen.get(tid, 0) != gen:
            return
        _set_async_exc(ctypes.c_ulong(tid), ctypes.py_object(SoftTimeLimitExceeded))


def run_sync(name, args, kwargs, soft_timeout_ms, meta=None):
    """Run one sync task and return its outcome dict."""
    try:
        return _run_sync_inner(name, args, kwargs, soft_timeout_ms, meta)
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
                    "origin": "worker",
                },
            }


def _run_sync_inner(name, args, kwargs, soft_timeout_ms, meta=None):
    td = _registry.get(name)
    if td is None:
        return {
            "ok": False,
            "retryable": False,
            "error": {
                "type": "UnregisteredTask",
                "message": "unknown task %s" % (name,),
                "traceback": "",
                "origin": "worker",
            },
        }
    fn = getattr(td, "fn")

    # Hooks see current_task() too, so a per task log scope gets the same id.
    ctx_token = _enter_context(name, meta)
    try:
        # Hooks run outside the armed soft timeout, so no injection lands in one.
        _run_hooks(_before_hooks, "before_task")
        tid = threading.get_ident()
        gen = _thread_gen.get(tid, 0)
        armed = soft_timeout_ms is not None and soft_timeout_ms > 0
        if armed:
            _schedule_soft_timeout(tid, gen, soft_timeout_ms)
        try:
            rv = fn(*args, **kwargs)
            out = _finish_value(rv)
        except BaseException as e:
            out = _finish_exc(e)
        finally:
            # The generation bump fences this call's deadline off the thread's
            # next task. A deadline firing between fn() returning and here is
            # inherent (PROTOCOL.md §4.6). Unarmed calls skip the cost.
            if armed:
                with _gen_lock:
                    _thread_gen[tid] = gen + 1
                _set_async_exc(ctypes.c_ulong(tid), None)
            _run_hooks(_after_hooks, "after_task")
    finally:
        _leave_context(ctx_token)
    return out


def _loop_main(loop):
    asyncio.set_event_loop(loop)
    loop.run_forever()


def _new_loop():
    """Build one event loop: uvloop when importable, else stock asyncio.

    CAULI_LOOP=asyncio forces the stock loop; CAULI_LOOP=uvloop makes uvloop
    mandatory and fails at startup without it.
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
            _deadline_heaps.append(_DeadlineHeap(loop))
            _cap_warned.append(False)
            _hb_sent.append(None)
            _hb_lag.append(0)
            _hb_sub.append(0)
            _hb_done.append(0)
    if impl is not None:
        sys.stderr.write("cauli: %d asyncio loop(s), impl=%s\n" % (n, impl))
    return len(_loops)


def set_callback(cb):
    global _callback
    _callback = cb


def _hb_ack(idx, sent):
    """Runs ON the loop thread: the only proof that it still runs callbacks."""
    _hb_lag[idx] = int((time.monotonic() - sent) * 1000)
    _hb_sent[idx] = None


def heartbeat():
    """Stamp every loop; return (lag_ms, outstanding, completed) per loop.

    `lag_ms` is the age of an unrun stamp or the latency of the last one.
    `outstanding` is submitted minus completed, so it includes Tasks created
    but never run. A wedge is a stale stamp, work outstanding, nothing finishing.
    """
    now = time.monotonic()
    out = []
    with _loops_lock:
        loops = list(_loops)
    for idx, loop in enumerate(loops):
        sent = _hb_sent[idx]
        if sent is None:
            lag_ms = _hb_lag[idx]
            # Before scheduling: the loop may ack before call_soon_threadsafe
            # returns, and a later write would read as forever outstanding.
            _hb_sent[idx] = now
            try:
                loop.call_soon_threadsafe(_hb_ack, idx, now)
            except RuntimeError:
                _hb_sent[idx] = None  # loop closed under us
        else:
            lag_ms = int((now - sent) * 1000)
        # Different writer threads: read in this order and floor at zero.
        submitted = _hb_sub[idx]
        completed = _hb_done[idx]
        out.append((lag_ms, max(0, submitted - completed), completed))
    return out


if sys.version_info >= (3, 12):

    def _start_task(loop, coro):
        """Start one `_arun` Task eagerly, so a body that never suspends skips a loop turn."""
        return asyncio.Task(coro, loop=loop, eager_start=True)

else:

    def _start_task(loop, coro):
        return loop.create_task(coro)


def _drain(idx, loop):
    """Turn one loop's queued submissions into Tasks. Runs ON that loop."""
    queue = _pending[idx]
    with _pending_locks[idx]:
        batch = queue[:]
        del queue[:]
    for token, name, args, kwargs, timeout_s, soft_timeout_s, meta in batch:
        # _arun never raises and always calls back, so no result handle is kept.
        _start_task(
            loop,
            _arun(idx, token, name, args, kwargs, timeout_s, soft_timeout_s, meta),
        )


def submit_async(token, name, args, kwargs, timeout_s, soft_timeout_s=None, meta=None):
    """Queue one async task on the next loop; raises AsyncQueueFull at _PENDING_CAP.

    The loop is woken only when its queue was empty: a wakeup per task, as
    run_coroutine_threadsafe does, dominates the async path's cost.
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
        _pending[idx].append(
            (token, name, args, kwargs, timeout_s, soft_timeout_s, meta)
        )
        _hb_sub[idx] += 1
        wake = len(_pending[idx]) == 1
    if wake:
        loop.call_soon_threadsafe(_drain, idx, loop)


# uvloop keeps loop time and timers in whole milliseconds, so the worker's
# own deadline can fire at a reading a float ulp below the one _arun took.
_DEADLINE_CLOCK_SLACK_S = 0.001


def _fire_deadline(task, fired):
    fired.append(True)
    task.cancel()


# asyncio's own heap compaction thresholds (base_events): memory stays within
# twice the live deadlines.
_DEADLINE_COMPACT_MIN = 100
_DEADLINE_COMPACT_FRACTION = 0.5


class _DeadlineHeap:
    """Every task deadline on one loop behind a single loop timer.

    A uvloop timer per task costs a libuv handle each. Cancelling only marks
    an entry; due entries fire in deadline order, ties in submission order.
    """

    __slots__ = ("_loop", "_heap", "_seq", "_cancelled", "_armed_at", "_timer")

    def __init__(self, loop):
        self._loop = loop
        self._heap = []
        self._seq = 0
        self._cancelled = 0
        self._armed_at = None
        self._timer = None

    def add(self, when, task, fired):
        self._seq += 1
        entry = [when, self._seq, task, fired]
        heapq.heappush(self._heap, entry)
        if self._armed_at is None or when < self._armed_at:
            self._arm(when)
        return entry

    def cancel(self, entry):
        if entry[2] is None:  # already fired
            return
        entry[2] = entry[3] = None
        self._cancelled += 1
        if (
            self._cancelled > _DEADLINE_COMPACT_MIN
            and self._cancelled > _DEADLINE_COMPACT_FRACTION * len(self._heap)
        ):
            self._heap = [e for e in self._heap if e[2] is not None]
            heapq.heapify(self._heap)
            self._cancelled = 0

    def _arm(self, when):
        if self._timer is not None:
            self._timer.cancel()
        self._timer = self._loop.call_at(when, self._fire)
        self._armed_at = when

    def _fire(self):
        self._timer = self._armed_at = None
        heap = self._heap
        due = self._loop.time() + _DEADLINE_CLOCK_SLACK_S
        while heap and heap[0][0] <= due:
            entry = heapq.heappop(heap)
            task, fired = entry[2], entry[3]
            if task is None:
                self._cancelled -= 1
                continue
            entry[2] = entry[3] = None
            _fire_deadline(task, fired)
        while heap and heap[0][2] is None:
            heapq.heappop(heap)
            self._cancelled -= 1
        if heap:
            self._arm(heap[0][0])


if sys.version_info >= (3, 12):

    async def _await_deadline(coro, timeout_s, deadlines):
        """3.12+ `asyncio.wait_for(coro, timeout_s)` semantics, deadline on `_DeadlineHeap`.

        Same cancel, uncancel and TimeoutError conversion as 3.12's wait_for,
        without its Timeout object and extra frames. Older Pythons keep the
        real wait_for, which is a different algorithm there.
        """
        task = asyncio.current_task()
        cancelling = task.cancelling()
        fired = []
        entry = deadlines.add(task.get_loop().time() + timeout_s, task, fired)
        cancelled = None
        try:
            return await coro
        except asyncio.CancelledError as exc:
            cancelled = exc
            raise
        finally:
            deadlines.cancel(entry)
            if fired and task.uncancel() <= cancelling and cancelled is not None:
                raise TimeoutError from cancelled

else:

    def _await_deadline(coro, timeout_s, deadlines):
        return asyncio.wait_for(coro, timeout_s)


async def _await_with_soft_limit(coro, soft_s, hard_s):
    """Run one async task body under the §4.6 soft then hard deadline pair.

    asyncio cannot throw an arbitrary exception into a coroutine, so the soft
    stop is `Task.cancel()`: the body sees `asyncio.CancelledError`, and the
    worker reports `SoftTimeLimitExceeded` as the other lanes do. A body that
    swallows the cancel keeps its value; one that runs past the hard budget
    times out as `TimeLimitExceeded`.
    """
    task = asyncio.ensure_future(coro)
    done, _ = await asyncio.wait((task,), timeout=soft_s)
    if done:
        return task.result()
    task.cancel()
    remaining = hard_s - soft_s
    done, _ = await asyncio.wait((task,), timeout=remaining if remaining > 0 else 0)
    if not done:
        # Hard limit. Cancel again, or the body runs on beside its own retry.
        task.cancel()
        raise asyncio.TimeoutError()
    if task.cancelled():
        raise SoftTimeLimitExceeded("soft time limit of %.3fs exceeded" % (soft_s,))
    return task.result()


async def _arun(
    idx, token, name, args, kwargs, timeout_s, soft_timeout_s=None, meta=None
):
    ctx_token = None
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
                    "origin": "worker",
                },
            }
        else:
            fn = getattr(td, "fn")
            # Each _arun is its own Task with its own context copy, so this
            # ContextVar reaches the body and no other coroutine on the loop.
            ctx_token = _enter_context(name, meta)
            # Hooks run outside the timeout. Guarded: an empty await still costs GIL time.
            if _before_hooks:
                await _run_hooks_async(_before_hooks, "before_task")
            deadline = asyncio.get_running_loop().time() + timeout_s
            try:
                if soft_timeout_s is None:
                    rv = await _await_deadline(
                        fn(*args, **kwargs), timeout_s, _deadline_heaps[idx]
                    )
                else:
                    rv = await _await_with_soft_limit(
                        fn(*args, **kwargs), soft_timeout_s, timeout_s
                    )
                out = _finish_value(rv)
            except (asyncio.TimeoutError, TimeoutError) as e:
                # From 3.11 both TimeoutErrors are one class, so the clock tells
                # the worker's limit from a TimeoutError the body raised itself.
                if (
                    asyncio.get_running_loop().time()
                    < deadline - _DEADLINE_CLOCK_SLACK_S
                ):
                    out = _finish_exc(e)
                else:
                    out = {
                        "ok": False,
                        "retryable": True,
                        "error": {
                            "type": "TimeLimitExceeded",
                            "message": "task timed out after %.3fs" % (timeout_s,),
                            "traceback": "",
                            "origin": "worker",
                        },
                    }
            except BaseException as e:
                out = _finish_exc(e)
            finally:
                if _after_hooks:
                    await _run_hooks_async(_after_hooks, "after_task")
    except BaseException as e:  # never lose a completion
        # Body and hook exceptions are caught inside, so this is the shim's own.
        out = {"ok": False, "retryable": True, "error": _error_dict(e, "worker")}
    finally:
        _leave_context(ctx_token)

    # Tells a slow loop from a wedged one. Only this loop's thread writes it.
    _hb_done[idx] += 1

    cb = _callback
    if cb is not None:
        try:
            cb(token, out)
        except Exception:
            traceback.print_exc()
