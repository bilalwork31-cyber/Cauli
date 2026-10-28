"""cauli._exec: the cpu child process (PROTOCOL.md section 5.1).

Fork server mode (default): the parent imports the app, runs ``gc.freeze()``
so children share the import image copy on write, and answers ``{"cmd":
"fork"}`` control lines with ``{"forked": pid}``. Each child connects to the
worker's unix socket and serves up to M requests at once, matched by ``id``.

Stdio mode (``--no-fork-server``): one request per stdin line, one response
per stdout line, one in flight.

fd 1 is pointed at stderr and the protocol stream kept on a private fd, so a
task's ``print()`` cannot corrupt the protocol. Task errors are reported,
never fatal; EOF exits 0; startup errors exit nonzero before the ready line.
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
import re
import signal
import socket
import sys
import threading
import time
import traceback
from typing import Any, TextIO

from cauli import _codec
from cauli._context import make_context, reset_current_task, set_current_task
from cauli._hooks import run_hooks
from cauli.exceptions import SoftTimeLimitExceeded

_TRACEBACK_CAP = 8192

# The cpu wire id is "{envelope id}.{hex seq}"; current_task() must report the
# bare envelope id the caller holds as AsyncResult.id.
_WIRE_SUFFIX_RE = re.compile(r"\.[0-9a-f]+\Z")

# The worker writes `id` first, so a line the codec rejects still names its
# request; without the id the task would hold its slot until the hard timeout.
_REQUEST_ID_RE = re.compile(r'\{"id":"([^"\\]*)"')

_set_async_exc = ctypes.pythonapi.PyThreadState_SetAsyncExc


def _format_traceback(exc: BaseException) -> str:
    text = "".join(traceback.format_exception(type(exc), exc, exc.__traceback__))
    if len(text) > _TRACEBACK_CAP:
        text = text[-_TRACEBACK_CAP:]
    return text


def _error_json(exc: BaseException, origin: str = "task") -> dict[str, Any]:
    # PROTOCOL.md section 8: "worker" only where cauli synthesized the error.
    return {
        "type": type(exc).__name__,
        "message": str(exc),
        "traceback": _format_traceback(exc),
        "origin": origin,
    }


def _on_alarm(signum: int, frame: Any) -> None:
    raise SoftTimeLimitExceeded("soft time limit exceeded")


def _is_retry(exc: BaseException) -> bool:
    """By name, not isinstance: must match shim.py and ctx.rs, which see names."""
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
    """Soft timeouts for threaded children (M > 1); SIGALRM reaches only the main thread.

    The per thread generation stops a stale deadline landing in a later
    request. The residual race is documented in PROTOCOL.md section 4.6.
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
                return
        _set_async_exc(ctypes.c_ulong(tid), ctypes.py_object(SoftTimeLimitExceeded))


async def _run_hooks_async(hooks: Any, where: str) -> None:
    """:func:`cauli._hooks.run_hooks` that awaits a hook returning an awaitable.

    The Django hook returns one so cleanup runs on asgiref's ORM thread.
    """
    for hook in hooks:
        try:
            r = hook()
            if r is not None and hasattr(r, "__await__"):
                await r
        except Exception:
            print(f"cauli: {where} hook {hook!r} raised (ignored):", file=sys.stderr)
            traceback.print_exc()


async def _run_async_task(
    app: Any,
    task: Any,
    args: Any,
    kwargs: Any,
    soft_timeout_ms: Any,
    watchdog: _SoftTimeoutWatchdog | None,
    use_alarm: bool,
) -> Any:
    """Run one ``async def`` cpu task with its hooks inside the event loop.

    A hook that branches on a running loop (Django's does) must see what the
    body sees, or the body's thread local connection is closed by neither.
    The soft timeout covers the body only, never a hook.
    """
    await _run_hooks_async(getattr(app, "_before_task_hooks", ()), "before_task")
    try:
        if use_alarm:
            signal.setitimer(signal.ITIMER_REAL, float(soft_timeout_ms) / 1000.0)
        elif watchdog is not None:
            watchdog.arm(float(soft_timeout_ms))
        try:
            return await task.fn(*args, **kwargs)
        finally:
            if use_alarm:
                signal.setitimer(signal.ITIMER_REAL, 0.0)
            elif watchdog is not None:
                watchdog.disarm()
    finally:
        await _run_hooks_async(getattr(app, "_after_task_hooks", ()), "after_task")


_loops = threading.local()


def _thread_event_loop() -> asyncio.AbstractEventLoop:
    """The persistent event loop of this thread; ``asyncio.run`` per task is costly.

    Per thread because both soft timeout schemes target the calling thread.
    ``set_event_loop`` matters: asgiref and Django read it. The pid guard
    stops a forked child reusing the parent's loop and its epoll fd.
    """
    loop = getattr(_loops, "loop", None)
    if loop is None or loop.is_closed() or getattr(_loops, "pid", None) != os.getpid():
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        _loops.loop = loop
        _loops.pid = os.getpid()
    return loop


def _cancel_orphans(loop: asyncio.AbstractEventLoop) -> None:
    """Cancel tasks the body left running, as ``asyncio.run`` would.

    The loop outlives the request, so an orphan would otherwise run inside
    the next task on this thread.
    """
    pending = [t for t in asyncio.all_tasks(loop) if not t.done()]
    if not pending:
        return
    for t in pending:
        t.cancel()
    loop.run_until_complete(asyncio.gather(*pending, return_exceptions=True))


def _execute(
    app: Any, request: dict[str, Any], watchdog: _SoftTimeoutWatchdog | None = None
) -> dict[str, Any]:
    """Run one task request and build the response dict. Never raises for task errors."""
    request_id = request.get("id")
    task_name = request.get("task")
    # Responses keep the wire suffix; current_task() gets the bare id.
    bare_id = request_id
    if isinstance(bare_id, str):
        bare_id = _WIRE_SUFFIX_RE.sub("", bare_id)
    args = request.get("args") or []
    kwargs = request.get("kwargs") or {}
    soft_timeout_ms = request.get("soft_timeout_ms")

    task = getattr(app, "_tasks", {}).get(task_name)
    if task is None:
        # Same type as the worker's own registry check (PROTOCOL.md section 8);
        # not retryable, since it can never succeed.
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
    # Hooks run outside the soft timeout, on every outcome (PROTOCOL.md 4.8).
    # The context is reset in the outer finally: threads are reused, and a
    # leak would show the next task the previous task's id.
    is_async = bool(getattr(task, "is_async", False))
    ctx_token = set_current_task(make_context(request, task_id=bare_id))
    try:
        if not is_async:
            run_hooks(getattr(app, "_before_task_hooks", ()), "before_task")
        try:
            if is_async:
                loop = _thread_event_loop()
                try:
                    result = loop.run_until_complete(
                        _run_async_task(
                            app,
                            task,
                            args,
                            kwargs,
                            soft_timeout_ms,
                            watchdog if use_watchdog else None,
                            use_alarm,
                        )
                    )
                finally:
                    _cancel_orphans(loop)
            else:
                if use_alarm:
                    signal.setitimer(
                        signal.ITIMER_REAL, float(soft_timeout_ms) / 1000.0
                    )
                elif use_watchdog:
                    watchdog.arm(float(soft_timeout_ms))
                try:
                    result = task.fn(*args, **kwargs)
                finally:
                    if use_alarm:
                        signal.setitimer(signal.ITIMER_REAL, 0.0)
                    elif use_watchdog:
                        watchdog.disarm()
        except BaseException as exc:
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
            # Stamped as on the io lanes; ctx.rs's name based default would
            # make a user "SerializationError" terminal here only.
            return {
                "id": request_id,
                "ok": False,
                "retryable": True,
                "error": _error_json(exc),
            }
        return {"id": request_id, "ok": True, "result": result}
    finally:
        if not is_async:
            run_hooks(getattr(app, "_after_task_hooks", ()), "after_task")
        reset_current_task(ctx_token)


def _handle_request_line(
    app: Any, line: str, watchdog: _SoftTimeoutWatchdog | None = None
) -> dict[str, Any]:
    """Decode one request line and execute it; always returns a response dict."""
    try:
        request = _codec.decode(line)
    except _codec.DECODE_ERRORS as exc:
        id_match = _REQUEST_ID_RE.match(line)
        return {
            "id": id_match.group(1) if id_match else None,
            "ok": False,
            "retryable": False,
            "error": {
                "type": "ProtocolError",
                "message": f"malformed request line: {exc}",
                "traceback": "",
                "origin": "worker",
            },
        }
    try:
        return _execute(app, request, watchdog=watchdog)
    except BaseException as exc:  # a soft timeout landing at the task boundary
        request_id = request.get("id") if isinstance(request, dict) else None
        return {
            "id": request_id,
            "ok": False,
            "retryable": True,
            "error": _error_json(exc),
        }


def _serialize_response_bytes(payload: dict[str, Any]) -> bytes:
    """One protocol line as UTF-8 bytes; an unencodable result becomes a SerializationError."""
    try:
        return _codec.encode_bytes(payload)
    except _codec.ENCODE_ERRORS as exc:
        error = {
            "type": "SerializationError",
            "message": f"task result is not JSON serializable: {exc}",
            "traceback": _format_traceback(exc),
            "origin": "worker",
        }
        return _codec.encode_bytes(
            {"id": payload.get("id"), "ok": False, "retryable": False, "error": error}
        )


def _serialize_response(payload: dict[str, Any]) -> str:
    """:func:`_serialize_response_bytes` as text, for stdio mode."""
    return _serialize_response_bytes(payload).decode("utf-8")


def _write_line(out: TextIO, payload: dict[str, Any]) -> None:
    out.write(_serialize_response(payload) + "\n")
    out.flush()


def _log(msg: str) -> None:
    print(msg, file=sys.stderr, flush=True)


def _set_pdeathsig() -> None:
    """PR_SET_PDEATHSIG=SIGKILL where prctl exists. fork() clears it, so each process arms it."""
    try:
        libc = ctypes.CDLL(None, use_errno=True)
        libc.prctl(1, signal.SIGKILL, 0, 0, 0)  # PR_SET_PDEATHSIG == 1
    except Exception:
        pass


def _ignore_stop(signum: int, frame: Any) -> None:
    """Ignore a group wide Ctrl+C or SIGTERM; the worker drains and ends this process.

    A handler, not SIG_IGN, because SIG_IGN survives exec into a task's subprocesses.
    """


def _restore_stop() -> None:
    signal.signal(signal.SIGINT, signal.default_int_handler)
    signal.signal(signal.SIGTERM, signal.SIG_DFL)


def _guard_stop_signals() -> None:
    signal.signal(signal.SIGINT, _ignore_stop)
    signal.signal(signal.SIGTERM, _ignore_stop)
    # Or a task's own forked child would ignore Process.terminate().
    os.register_at_fork(after_in_child=_restore_stop)


def _rss_kb() -> tuple[int | None, int | None]:
    """(VmRSS kB, private kB) from /proc for the startup log; Nones off Linux."""
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


def _reap_children(signum: int, frame: Any) -> None:
    """SIGCHLD: reap children and log abnormal exits.

    The worker sees only EOF; only this waitpid status can tell a segfault
    or OOM kill apart.
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
    proto_fd = os.dup(1)
    os.dup2(2, 1)
    proto = os.fdopen(proto_fd, "w", encoding="utf-8", newline="\n")
    sys.stdout = sys.stderr
    try:
        sys.stdin.reconfigure(encoding="utf-8", errors="replace")  # type: ignore[union-attr]
    except Exception:
        pass

    _set_pdeathsig()
    _guard_stop_signals()
    app = _load_app(app_spec)

    # Before the first fork: a connection opened at import would otherwise be
    # one socket fd shared by every child, corrupting its stream.
    run_hooks(getattr(app, "_process_init_hooks", ()), "process_init")

    # Children's GC never scans frozen objects, so never copies their pages.
    gc.collect()
    gc.freeze()

    signal.signal(signal.SIGCHLD, _reap_children)

    # Only the forking thread survives fork; a lock another thread holds
    # stays locked forever in every child.
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
        if line == "":
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
                _guard_stop_signals()
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
    # Closing the control fd gives the worker a clean EOF on parent exit;
    # /dev/null on fd 0 stops task code eating control bytes.
    os.close(parent_proto_fd)
    devnull = os.open(os.devnull, os.O_RDONLY)
    os.dup2(devnull, 0)
    os.close(devnull)

    # Or every sibling inherits the same `random` state and sequence.
    random.seed()

    _set_pdeathsig()
    if os.getppid() == 1:
        return 0  # the parent died inside the fork window
    signal.signal(signal.SIGCHLD, signal.SIG_DFL)

    # Again, for resources created between the parent's run and the fork.
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
        return 0


def _safe_handle_and_respond(
    app: Any,
    line: str,
    writer: "_SocketWriter",
    watchdog: _SoftTimeoutWatchdog | None = None,
) -> None:
    """`_handle_request_line` + `writer.write_response`, guarded end to end.

    A soft timeout injected while the response is serialized or sent would
    otherwise kill the thread or the child, leaving the request unanswered.
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
    except BaseException as exc:
        # The worker will only learn of this task at its hard timeout.
        _log(
            "cauli._exec: response write failed for request "
            f"{resp.get('id')!r}: {exc!r}"
        )
    if watchdog is not None:
        # Move the generation on even on the exception path (PROTOCOL.md 4.6).
        watchdog.disarm()


class _SocketWriter:
    """Thread-safe response line writer over the child's unix socket."""

    def __init__(self, sock: socket.socket) -> None:
        self._sock = sock
        self._lock = threading.Lock()

    def write_response(self, payload: dict[str, Any]) -> None:
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
        if line == "":
            return 0
        line = line.strip()
        if not line:
            continue
        _safe_handle_and_respond(app, line, writer)


def _serve_socket_threaded(app: Any, sock: socket.socket, threads: int) -> int:
    """M > 1: M worker threads; responses leave out of order, matched by id."""
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
        if line == "":
            return 0
        line = line.strip()
        if line:
            requests.put(line)


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

    proto_fd = os.dup(1)
    os.dup2(2, 1)
    proto = os.fdopen(proto_fd, "w", encoding="utf-8", newline="\n")
    sys.stdout = sys.stderr

    try:
        sys.stdin.reconfigure(encoding="utf-8", errors="replace")  # type: ignore[union-attr]
    except Exception:
        pass

    _guard_stop_signals()
    app = _load_app(ns.app)
    run_hooks(getattr(app, "_process_init_hooks", ()), "process_init")

    if hasattr(signal, "SIGALRM"):
        signal.signal(signal.SIGALRM, _on_alarm)

    _write_line(proto, {"ready": True, "pid": os.getpid()})

    while True:
        line = sys.stdin.readline()
        if line == "":
            break
        line = line.strip()
        if not line:
            continue
        _write_line(proto, _handle_request_line(app, line))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
