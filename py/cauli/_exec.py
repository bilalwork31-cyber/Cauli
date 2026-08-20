"""cauli._exec: the cpu child process (PROTOCOL.md section 5.1).

Two modes, one request/response line protocol:

**Fork-server mode** (the worker's default):
``{python} -m cauli._exec --app module:attr --fork-server --connect <unix
socket path> [--child-threads M]``. The PARENT imports the app once, runs
``gc.collect()`` + ``gc.freeze()`` (so forked children share the warmed
import image copy-on-write and the GC never dirties frozen pages), then
serves fork requests over its stdin/stdout control channel: one
``{"cmd": "fork"}`` line in, one ``{"forked": <pid>}`` line out. It reaps
children via SIGCHLD, exits 0 on stdin EOF, and sets PR_SET_PDEATHSIG so it
dies with the worker. Each forked CHILD connects to the worker's unix socket
listener, sends ``{"ready": true, "pid": N, "concurrency": M}`` on that
connection, then serves requests on it -- up to M in flight at once
(responses matched by ``id``, may be out of order). Soft timeouts: SIGALRM
when M == 1, a shared watchdog thread + PyThreadState_SetAsyncExc when
M > 1 (SIGALRM only ever fires in the main thread).

**Stdio mode** (fallback, ``--no-fork-server`` worker flag): spawned as
``{python} -m cauli._exec --app module:attr``. Prints exactly one ready line
``{"ready": true, "pid": N}`` on stdout, then reads one JSON request per
line from stdin and writes one JSON response per line to stdout, one request
in flight, flushing after every line. stderr is passthrough logging.

Robustness notes:
- In stdio mode fd 1 is re-pointed at stderr right after startup and the
  original stdout pipe is kept on a private fd, so task-level ``print()``
  (or even C-level writes to fd 1) cannot corrupt the line protocol. The
  fork-server parent does the same for its control channel; forked children
  additionally point fd 0 at /dev/null and close the inherited control fd.
- The child never crashes on task exceptions; it reports them. EOF on
  stdin/socket exits 0. Import/startup errors exit nonzero before the
  ready/server line.
"""

from __future__ import annotations

import argparse
import asyncio
import ctypes
import gc
import heapq
import importlib
import os
import queue
import random
import signal
import socket
import sys
import threading
import time
import traceback
from typing import Any, TextIO

from cauli import _codec
from cauli._hooks import run_hooks
from cauli.exceptions import SoftTimeLimitExceeded

_TRACEBACK_CAP = 8192  # max chars of formatted traceback kept in error JSON (section 8)

_set_async_exc = ctypes.pythonapi.PyThreadState_SetAsyncExc


def _format_traceback(exc: BaseException) -> str:
    text = "".join(traceback.format_exception(type(exc), exc, exc.__traceback__))
    if len(text) > _TRACEBACK_CAP:
        text = text[-_TRACEBACK_CAP:]
    return text


def _error_json(exc: BaseException, origin: str = "task") -> dict[str, Any]:
    # PROTOCOL.md section 8 `origin`, same rule as worker/src/shim.py: "task"
    # whenever a real exception ended the task invocation, which includes a
    # propagated SoftTimeLimitExceeded (the child injected it, but it did
    # leave user code). "worker" is passed explicitly where cauli itself
    # synthesized the error with no task exception behind it.
    return {
        "type": type(exc).__name__,
        "message": str(exc),
        "traceback": _format_traceback(exc),
        "origin": origin,
    }


def _on_alarm(signum: int, frame: Any) -> None:
    raise SoftTimeLimitExceeded("soft time limit exceeded")


def _is_retry(exc: BaseException) -> bool:
    """Duck-typed retry recognition (audit M6): an exception class named
    exactly "Retry" exposing a ``.countdown`` attribute is treated as a
    forced retry. This mirrors worker/src/shim.py's `_is_retry` exactly (by
    name, not `isinstance`), so cpu and io tasks agree on the SAME rule
    regardless of which `Retry` class raised it -- the app's own `cauli.Retry`,
    or an embedded duck-type in a test fixture with no cauli import at all.
    Previously this module used `isinstance(exc, cauli.exceptions.Retry)`,
    which silently disagreed with the shim's name-based rule and with the
    Rust cpu-mapping (ctx.rs), which only ever sees the type name string.
    """
    return type(exc).__name__ == "Retry" and hasattr(exc, "countdown")


def _load_app(spec: str) -> Any:
    module_name, sep, attr = spec.partition(":")
    if not sep or not module_name or not attr:
        print(
            f"cauli._exec: invalid --app {spec!r} (expected module:attr)",
            file=sys.stderr,
        )
        raise SystemExit(2)
    cwd = os.getcwd()
    if cwd not in sys.path:
        sys.path.insert(0, cwd)
    module = importlib.import_module(module_name)
    try:
        return getattr(module, attr)
    except AttributeError:
        print(
            f"cauli._exec: module {module_name!r} has no attribute {attr!r}",
            file=sys.stderr,
        )
        raise SystemExit(2) from None


class _SoftTimeoutWatchdog:
    """Shared soft-timeout watchdog for threaded children (M > 1).

    SIGALRM is only ever delivered to a process's main thread, so the M=1
    setitimer scheme cannot arm per-request soft timeouts once M worker
    threads execute concurrently. This mirrors the worker shim's pattern
    (worker/src/shim.py, audit MEM-4/M3): ONE daemon thread services a
    min-heap of ``(deadline, tid, generation)`` and injects
    ``SoftTimeLimitExceeded`` via ``PyThreadState_SetAsyncExc``; a per-thread
    generation counter fences stale deadlines so one can never land inside a
    LATER request on the same worker thread. The residual race (a deadline
    firing between the task returning and its ``finally`` disarm) is
    inherent to async-exc injection, exactly as documented for the io path
    in PROTOCOL.md section 4.6.
    """

    def __init__(self) -> None:
        self._heap: list[tuple[float, int, int]] = []
        self._cond = threading.Condition()
        self._gen: dict[int, int] = {}
        self._gen_lock = threading.Lock()
        self._thread: threading.Thread | None = None

    def arm(self, soft_timeout_ms: float) -> None:
        tid = threading.get_ident()
        with self._gen_lock:
            gen = self._gen.get(tid, 0)
        deadline = time.monotonic() + soft_timeout_ms / 1000.0
        with self._cond:
            if self._thread is None:
                self._thread = threading.Thread(
                    target=self._loop, name="cauli-exec-watchdog", daemon=True
                )
                self._thread.start()
            heapq.heappush(self._heap, (deadline, tid, gen))
            self._cond.notify()

    def disarm(self) -> None:
        tid = threading.get_ident()
        with self._gen_lock:
            self._gen[tid] = self._gen.get(tid, 0) + 1
        _set_async_exc(ctypes.c_ulong(tid), None)

    def _loop(self) -> None:
        while True:
            with self._cond:
                while not self._heap:
                    self._cond.wait()
                deadline, tid, gen = self._heap[0]
                remaining = deadline - time.monotonic()
                if remaining > 0:
                    self._cond.wait(timeout=remaining)
                    continue
                heapq.heappop(self._heap)
            self._inject(tid, gen)

    def _inject(self, tid: int, gen: int) -> None:
        with self._gen_lock:
            if self._gen.get(tid, 0) != gen:
                return  # that request already finished: stale deadline
        _set_async_exc(ctypes.c_ulong(tid), ctypes.py_object(SoftTimeLimitExceeded))


def _execute(
    app: Any, request: dict[str, Any], watchdog: _SoftTimeoutWatchdog | None = None
) -> dict[str, Any]:
    """Run one task request and build the response dict. Never raises for task errors.

    Soft timeout: SIGALRM/setitimer when ``watchdog`` is None (single-threaded
    execution in the main thread), else the shared watchdog thread.
    """
    request_id = request.get("id")
    task_name = request.get("task")
    args = request.get("args") or []
    kwargs = request.get("kwargs") or {}
    soft_timeout_ms = request.get("soft_timeout_ms")

    task = getattr(app, "_tasks", {}).get(task_name)
    if task is None:
        # Same error.type string as the worker's own pre dispatch registry
        # check (worker/src/dispatch.rs, PROTOCOL.md section 8), and
        # "retryable": False so this fails on this one attempt instead of
        # burning the full backoff schedule on something that can never
        # succeed: worker/src/ctx.rs's parse_pyresp honors an explicit
        # "retryable" field over its own default.
        return {
            "id": request_id,
            "ok": False,
            "retryable": False,
            "error": {
                "type": "UnregisteredTask",
                "message": f"task {task_name!r} is not registered in this app",
                "traceback": "",
                "origin": "worker",
            },
        }

    use_alarm = (
        bool(soft_timeout_ms) and watchdog is None and hasattr(signal, "setitimer")
    )
    use_watchdog = bool(soft_timeout_ms) and watchdog is not None
    # Per-task lifecycle hooks (PROTOCOL.md section 4.8): before hooks run on
    # THIS thread before the soft timeout is armed (hook time is not charged
    # against the task's soft budget, and an injection cannot land inside a
    # hook); after hooks run in the outer finally, after the disarm, on every
    # outcome path (success, task exception, forced retry).
    run_hooks(getattr(app, "_before_task_hooks", ()), "before_task")
    try:
        try:
            if use_alarm:
                signal.setitimer(signal.ITIMER_REAL, float(soft_timeout_ms) / 1000.0)
            elif use_watchdog:
                watchdog.arm(float(soft_timeout_ms))
            try:
                if task.is_async:
                    result = asyncio.run(task.fn(*args, **kwargs))
                else:
                    result = task.fn(*args, **kwargs)
            finally:
                if use_alarm:
                    signal.setitimer(signal.ITIMER_REAL, 0.0)
                elif use_watchdog:
                    watchdog.disarm()
        except BaseException as exc:  # the child must never crash on task errors
            if _is_retry(exc):
                cd = getattr(exc, "countdown", None)
                if cd is not None:
                    try:
                        cd = float(cd)
                    except Exception:
                        cd = None
                return {
                    "id": request_id,
                    "ok": False,
                    "retry": True,
                    "countdown": cd,
                    "error": _error_json(exc),
                }
            # Stamped, exactly as shim.py's `_finish_exc` stamps the io lanes:
            # left absent, ctx.rs falls back to a name based default that makes
            # a user exception class named "SerializationError" terminal here
            # and retryable on io, for the same task and the same exception.
            return {
                "id": request_id,
                "ok": False,
                "retryable": True,
                "error": _error_json(exc),
            }
        return {"id": request_id, "ok": True, "result": result}
    finally:
        run_hooks(getattr(app, "_after_task_hooks", ()), "after_task")


def _handle_request_line(
    app: Any, line: str, watchdog: _SoftTimeoutWatchdog | None = None
) -> dict[str, Any]:
    """Decode one request line and execute it; always returns a response dict."""
    try:
        request = _codec.decode(line)
    except _codec.DECODE_ERRORS as exc:
        return {
            "id": None,
            "ok": False,
            "error": {
                "type": "ProtocolError",
                "message": f"malformed request line: {exc}",
                "traceback": "",
                "origin": "worker",
            },
        }
    try:
        return _execute(app, request, watchdog=watchdog)
    except BaseException as exc:  # e.g. a soft timeout landing at the task boundary
        request_id = request.get("id") if isinstance(request, dict) else None
        return {
            "id": request_id,
            "ok": False,
            "retryable": True,
            "error": _error_json(exc),
        }


def _serialize_response_bytes(payload: dict[str, Any]) -> bytes:
    """Serialize one protocol line as UTF-8 bytes (no trailing newline).

    A non JSON serializable result degrades to a SerializationError response
    for the same request id (section 5.1).

    Bytes, not str: this is the socket path's serializer and ``sendall`` wants
    bytes. Producing str here cost a full transcode of every single response
    (see ``_codec.encode_bytes``).
    """
    try:
        return _codec.encode_bytes(payload)
    except _codec.ENCODE_ERRORS as exc:
        error = {
            "type": "SerializationError",
            "message": f"task result is not JSON serializable: {exc}",
            "traceback": _format_traceback(exc),
            # The task itself succeeded and returned; the codec is what
            # failed, so this error object is cauli's, not the task's.
            "origin": "worker",
        }
        # Stamped rather than left to the reader's name based default, which
        # is only a fallback for older children (PROTOCOL.md section 8).
        return _codec.encode_bytes(
            {"id": payload.get("id"), "ok": False, "retryable": False, "error": error}
        )


def _serialize_response(payload: dict[str, Any]) -> str:
    """:func:`_serialize_response_bytes` as text, for the stdio fallback mode's
    text-mode protocol stream (not a hot path; the socket path uses bytes)."""
    return _serialize_response_bytes(payload).decode("utf-8")


def _write_line(out: TextIO, payload: dict[str, Any]) -> None:
    """Serialize and emit one protocol line, always flushing."""
    out.write(_serialize_response(payload) + "\n")
    out.flush()


def _log(msg: str) -> None:
    print(msg, file=sys.stderr, flush=True)


def _set_pdeathsig() -> None:
    """Best-effort PR_SET_PDEATHSIG=SIGKILL (Linux): die with our parent.

    Cleared by fork(), so the fork-server parent AND every forked child each
    arm it for themselves (parent dies with the worker; children die with
    the parent). No-op on platforms without prctl.
    """
    try:
        libc = ctypes.CDLL(None, use_errno=True)
        libc.prctl(1, signal.SIGKILL, 0, 0, 0)  # PR_SET_PDEATHSIG == 1
    except Exception:
        pass


def _rss_kb() -> tuple[int | None, int | None]:
    """(VmRSS kB, private (dirty+clean) kB) from /proc; Nones off-Linux.

    Report-only numbers: the private figure is what a forked child actually
    adds on top of the shared parent image (the CoW win gc.freeze protects).
    """
    rss = private = None
    try:
        with open("/proc/self/status") as f:
            for line in f:
                if line.startswith("VmRSS:"):
                    rss = int(line.split()[1])
                    break
    except Exception:
        pass
    try:
        with open("/proc/self/smaps_rollup") as f:
            for line in f:
                if line.startswith(("Private_Dirty:", "Private_Clean:")):
                    private = (private or 0) + int(line.split()[1])
    except Exception:
        pass
    return rss, private


# --------------------------------------------------------------------------
# fork-server mode
# --------------------------------------------------------------------------


def _reap_children(signum: int, frame: Any) -> None:
    """SIGCHLD handler in the fork-server parent: reap every exited child.

    Logs WIFSIGNALED and the signal number for an abnormal exit. cpu.rs
    learns of a child's death from EOF on that child's own socket
    connection, which cannot carry a reason: the process is already gone by
    the time EOF is observed, so it cannot self report why. Only this
    parent's waitpid() status, once reaped, can tell a segfault, an OOM
    kill, or any other unprompted signal death apart from a plain nonzero
    exit. A normal exit (status 0) stays silent, same as before.
    """
    while True:
        try:
            pid, status = os.waitpid(-1, os.WNOHANG)
        except ChildProcessError:
            return
        if pid == 0:
            return
        if os.WIFSIGNALED(status):
            sig = os.WTERMSIG(status)
            try:
                name = signal.Signals(sig).name
            except ValueError:
                name = "?"
            _log(f"cauli._exec: child pid={pid} killed by signal {sig} ({name})")
        elif os.WIFEXITED(status) and os.WEXITSTATUS(status) != 0:
            _log(
                f"cauli._exec: child pid={pid} exited with code {os.WEXITSTATUS(status)}"
            )


def _fork_server_main(app_spec: str, sock_path: str, child_threads: int) -> int:
    """The fork-server PARENT: import once, freeze, serve fork requests."""
    # Reserve the real stdout for control replies; stray prints from the app
    # import (or anything else) go to stderr, exactly like stdio mode.
    proto_fd = os.dup(1)
    os.dup2(2, 1)
    proto = os.fdopen(proto_fd, "w", encoding="utf-8", newline="\n")
    sys.stdout = sys.stderr
    try:
        sys.stdin.reconfigure(encoding="utf-8", errors="replace")  # type: ignore[union-attr]
    except Exception:
        pass

    _set_pdeathsig()
    app = _load_app(app_spec)  # import errors: nonzero exit before the server line

    # Process-init hooks run in the parent BEFORE the first fork (PROTOCOL.md
    # section 4.8): resources opened as an import side effect (e.g. a Django
    # DB connection created by a module-level query) must be closed here, or
    # every forked child would inherit the SAME underlying socket fd and two
    # children writing to it would corrupt the stream. Each forked child runs
    # them again after fork (belt and suspenders; a no-op when the parent
    # already cleaned up).
    run_hooks(getattr(app, "_process_init_hooks", ()), "process_init")

    # The CoW payoff: collect import-time garbage once, then freeze every
    # object created so far into the permanent generation. Children fork from
    # this warmed image; their (default, enabled) GC never scans -- and so
    # never CoW-dirties the refcount/gc-header pages of -- frozen objects.
    gc.collect()
    gc.freeze()

    signal.signal(signal.SIGCHLD, _reap_children)

    # FS-9 diagnostic: forking a multi-threaded process is a classic footgun
    # (only the forking thread survives in the child; a lock held by any
    # other thread at fork time stays locked forever in every child). The
    # app import above is the only thing that could have started a
    # background thread before this point -- warn loudly rather than let it
    # surface later as an unexplained per-child deadlock.
    if threading.active_count() > 1:
        _log(
            f"cauli._exec: WARNING: {threading.active_count()} threads active "
            "at fork-server startup (app import started a background thread?); "
            "every forked child inherits only the forking thread and can "
            "deadlock on a lock held by one of the others at fork time"
        )

    _write_line(proto, {"server": True, "pid": os.getpid()})
    rss, _private = _rss_kb()
    _log(
        f"cauli._exec: fork-server parent ready pid={os.getpid()} "
        f"rss_kb={rss} frozen={gc.get_freeze_count()} child_threads={child_threads}"
    )

    while True:
        line = sys.stdin.readline()
        if line == "":  # EOF: the worker closed the control channel
            break
        line = line.strip()
        if not line:
            continue
        try:
            cmd = _codec.decode(line)
        except _codec.DECODE_ERRORS as exc:
            _write_line(proto, {"error": f"malformed control line: {exc}"})
            continue
        if isinstance(cmd, dict) and cmd.get("cmd") == "fork":
            try:
                pid = os.fork()
            except OSError as exc:
                _write_line(proto, {"error": f"fork failed: {exc}"})
                continue
            if pid == 0:
                code = 1
                try:
                    code = _forked_child_main(app, proto_fd, sock_path, child_threads)
                except BaseException:
                    traceback.print_exc()
                finally:
                    os._exit(code)
            _write_line(proto, {"forked": pid})
        else:
            _write_line(proto, {"error": f"unknown control command: {line[:200]}"})
    return 0


def _forked_child_main(
    app: Any, parent_proto_fd: int, sock_path: str, child_threads: int
) -> int:
    """A forked CHILD: connect to the worker's unix socket and serve requests."""
    # Drop the parent's control channel: close the inherited control-stdout
    # dup (so a parent exit yields clean EOF for the worker) and point fd 0
    # at /dev/null (so task code reading stdin cannot eat control bytes).
    # fd 1 already points at stderr -- the parent re-pointed it pre-fork.
    os.close(parent_proto_fd)
    devnull = os.open(os.devnull, os.O_RDONLY)
    os.dup2(devnull, 0)
    os.close(devnull)

    # FS-9: every child forked from the same warmed parent otherwise inherits
    # the IDENTICAL `random` module state, so unseeded `random` calls (jitter,
    # sampling, non-crypto ids) produce the SAME sequence in every sibling.
    # Reseed from OS entropy right after fork, before any task can run.
    # secrets/uuid4 are unaffected either way (both read urandom directly).
    random.seed()

    _set_pdeathsig()  # fork cleared it; re-arm against the fork-server parent
    if os.getppid() == 1:
        return 0  # the parent died inside the fork window; nothing to serve
    signal.signal(signal.SIGCHLD, signal.SIG_DFL)

    # Process-init hooks, again, in THIS child (PROTOCOL.md section 4.8):
    # the parent already ran them pre-fork, so anything reaching this call is
    # a resource created between then and the fork — or a hook that is simply
    # idempotent (the Django contrib's connections.close_all() is).
    run_hooks(getattr(app, "_process_init_hooks", ()), "process_init")

    sock = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
    sock.connect(sock_path)

    rss, private = _rss_kb()
    _log(
        f"cauli._exec: fork child pid={os.getpid()} rss_kb={rss} "
        f"private_kb={private} frozen={gc.get_freeze_count()}"
    )

    try:
        if child_threads <= 1:
            return _serve_socket_single(app, sock)
        return _serve_socket_threaded(app, sock, child_threads)
    except (BrokenPipeError, ConnectionResetError):
        return 0  # the worker went away mid-write; normal on kill paths


def _safe_handle_and_respond(
    app: Any,
    line: str,
    writer: "_SocketWriter",
    watchdog: _SoftTimeoutWatchdog | None = None,
) -> None:
    """`_handle_request_line` + `writer.write_response`, guarded end to end.

    FS-5: a soft-timeout injection can land between `_handle_request_line`
    returning and the response actually being written (during
    `_serialize_response`'s json.dumps, or inside `sendall`) -- that window
    is outside `_handle_request_line`'s own BaseException guard. Left
    unguarded, it silently kills the calling thread (M > 1: one of the M
    worker threads, with its in-flight request never answered until the
    caller's own hard timeout) or the whole child (M == 1). Mirrors the
    defensive `except BaseException` shim.py already uses around every
    Python-visible completion path on the io side.
    """
    try:
        resp = _handle_request_line(app, line, watchdog=watchdog)
    except BaseException as exc:
        try:
            req = _codec.decode(line)
            rid = req.get("id") if isinstance(req, dict) else None
        except Exception:
            rid = None
        resp = {"id": rid, "ok": False, "retryable": True, "error": _error_json(exc)}
    try:
        writer.write_response(resp)
    except BaseException:
        pass  # connection likely gone; the caller's read-EOF check ends serving
    if watchdog is not None:
        # Belt-and-suspenders: make sure this thread's generation has moved
        # on before it picks up another request, even on the exception path
        # above (narrows the residual same-generation race a hair further;
        # see PROTOCOL.md section 4.6).
        watchdog.disarm()


class _SocketWriter:
    """Thread-safe response line writer over the child's unix socket."""

    def __init__(self, sock: socket.socket) -> None:
        self._sock = sock
        self._lock = threading.Lock()

    def write_response(self, payload: dict[str, Any]) -> None:
        # Straight to bytes: no str round trip, one concat, one sendall.
        data = _serialize_response_bytes(payload) + b"\n"
        with self._lock:
            self._sock.sendall(data)


def _serve_socket_single(app: Any, sock: socket.socket) -> int:
    """M == 1: one request in flight, SIGALRM soft timeouts (main thread)."""
    if hasattr(signal, "SIGALRM"):
        signal.signal(signal.SIGALRM, _on_alarm)
    writer = _SocketWriter(sock)
    writer.write_response({"ready": True, "pid": os.getpid(), "concurrency": 1})
    rfile = sock.makefile("r", encoding="utf-8", errors="replace", newline="\n")
    while True:
        line = rfile.readline()
        if line == "":  # worker closed the connection
            return 0
        line = line.strip()
        if not line:
            continue
        _safe_handle_and_respond(app, line, writer)


def _serve_socket_threaded(app: Any, sock: socket.socket, threads: int) -> int:
    """M > 1: up to M requests in flight on M worker threads; responses may
    leave out of order and are matched by id. Soft timeouts via the shared
    watchdog (SIGALRM cannot serve worker threads)."""
    writer = _SocketWriter(sock)
    writer.write_response({"ready": True, "pid": os.getpid(), "concurrency": threads})
    watchdog = _SoftTimeoutWatchdog()
    requests: queue.Queue[str] = queue.Queue()

    def worker_loop() -> None:
        while True:
            line = requests.get()
            _safe_handle_and_respond(app, line, writer, watchdog=watchdog)

    for i in range(threads):
        threading.Thread(
            target=worker_loop, name=f"cauli-exec-worker-{i}", daemon=True
        ).start()

    rfile = sock.makefile("r", encoding="utf-8", errors="replace", newline="\n")
    while True:
        line = rfile.readline()
        if line == "":  # EOF: daemon worker threads die with the process
            return 0
        line = line.strip()
        if line:
            requests.put(line)


# --------------------------------------------------------------------------
# stdio mode (fallback) + entry point
# --------------------------------------------------------------------------


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        prog="python -m cauli._exec", description="cauli cpu child process"
    )
    parser.add_argument("--app", required=True, metavar="module:attr")
    parser.add_argument(
        "--fork-server",
        action="store_true",
        help="run as the fork-server parent (requires --connect)",
    )
    parser.add_argument(
        "--connect",
        metavar="SOCKET_PATH",
        help="unix socket path forked children connect back to",
    )
    parser.add_argument(
        "--child-threads",
        type=int,
        default=1,
        metavar="M",
        help="worker threads per forked child (its advertised concurrency)",
    )
    ns = parser.parse_args(argv)

    if ns.fork_server:
        if not ns.connect:
            print(
                "cauli._exec: --fork-server requires --connect <unix socket path>",
                file=sys.stderr,
            )
            return 2
        if not hasattr(os, "fork"):
            print(
                "cauli._exec: --fork-server requires a platform with os.fork()",
                file=sys.stderr,
            )
            return 2
        return _fork_server_main(ns.app, ns.connect, max(1, ns.child_threads))

    # Reserve the real stdout pipe for protocol lines; point fd 1 at stderr so
    # stray prints from tasks (or C extensions) go to passthrough logging.
    proto_fd = os.dup(1)
    os.dup2(2, 1)
    proto = os.fdopen(proto_fd, "w", encoding="utf-8", newline="\n")
    sys.stdout = sys.stderr

    try:
        sys.stdin.reconfigure(encoding="utf-8", errors="replace")  # type: ignore[union-attr]
    except Exception:
        pass

    app = _load_app(ns.app)  # import errors propagate: nonzero exit before ready line

    # Process-init hooks (PROTOCOL.md section 4.8): once per stdio child,
    # after the app import, before any task can execute.
    run_hooks(getattr(app, "_process_init_hooks", ()), "process_init")

    if hasattr(signal, "SIGALRM"):
        signal.signal(signal.SIGALRM, _on_alarm)

    _write_line(proto, {"ready": True, "pid": os.getpid()})

    while True:
        line = sys.stdin.readline()
        if line == "":  # EOF: parent closed the pipe, exit cleanly
            break
        line = line.strip()
        if not line:
            continue
        _write_line(proto, _handle_request_line(app, line))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
