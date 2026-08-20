#!/usr/bin/env python3
"""Standalone stand-in for `python3 -m cauli._exec` (PROTOCOL §5.1).

Used by e2e tests via the documented CAULI_EXEC_CMD env hook so the cpu pool
can be exercised without the real cauli package. Implements BOTH modes:

- stdio mode (no --fork-server flag): ready line, then one JSON request per
  line on stdin, one JSON response per line on stdout, one in flight.
- fork-server mode (--fork-server --connect PATH [--child-threads M]):
  `{"server": true, "pid": N}` on stdout, then `{"cmd":"fork"}` control
  lines each answered with `{"forked": pid}`; forked children connect to
  PATH, send `{"ready": true, "pid": N, "concurrency": M}` and serve up to
  M concurrent requests (threaded when M > 1, responses matched by id).

Never crashes on task exceptions; reports them. Hard timeouts are enforced
worker-side (SIGKILL), which the fx.cpu_slow task exists to provoke. Soft
timeouts mirror the real cauli._exec: SIGALRM when single threaded, a
threading.Timer + PyThreadState_SetAsyncExc injection when threaded (a
fixture-grade simplification of the real shared-watchdog pattern).
"""

import ctypes
import json
import os
import queue
import signal
import socket
import sys
import threading
import time


class SoftTimeLimitExceeded(Exception):
    """Name matches the real cauli exception (the worker maps it by type name)."""


def handle(task, args, kwargs):
    if task == "fx.cpu_echo":
        return {"args": args, "kwargs": kwargs, "pid": os.getpid()}
    if task == "fx.cpu_slow":
        time.sleep(float(args[0]) if args else 30.0)
        return "slow-done"
    if task == "fx.cpu_slow_pid":
        time.sleep(float(args[0]) if args else 1.0)
        return {"pid": os.getpid(), "tid": threading.get_ident()}
    if task == "fx.cpu_soft_slow":
        # sliced sleep so an injected async exception lands promptly
        end = time.monotonic() + (float(args[0]) if args else 5.0)
        while time.monotonic() < end:
            time.sleep(0.05)
        return "soft-done"
    if task == "fx.cpu_die_once":
        counter_file = args[0]
        if not os.path.exists(counter_file):
            with open(counter_file, "w") as f:
                f.write("died")
            os._exit(9)  # simulates a crashing child mid-task
        return "revived"
    if task == "fx.cpu_die_always":
        os._exit(9)  # simulates a child that never survives a single task
    if task == "fx.cpu_selfsignal":
        sig = int(args[0]) if args else signal.SIGSEGV
        os.kill(os.getpid(), sig)  # simulates a segfault or an OOM kill
        return "unreachable"
    if task == "fx.cpu_fail":
        raise ValueError("cpu boom")
    if task == "fx.cpu_ghost":
        return {"echo": "ghost-real"}
    raise KeyError("unknown fake task %r" % (task,))


def _on_alarm(signum, frame):
    raise SoftTimeLimitExceeded("soft time limit exceeded")


def run_request(req, threaded):
    """Execute one request dict -> response dict (soft timeout included)."""
    rid = req.get("id")
    soft_ms = req.get("soft_timeout_ms")
    timer = None
    armed = [True]
    if soft_ms and threaded:
        tid = threading.get_ident()

        def _inject():
            if armed[0]:
                ctypes.pythonapi.PyThreadState_SetAsyncExc(
                    ctypes.c_ulong(tid), ctypes.py_object(SoftTimeLimitExceeded)
                )

        timer = threading.Timer(float(soft_ms) / 1000.0, _inject)
        timer.start()
    elif soft_ms:
        signal.setitimer(signal.ITIMER_REAL, float(soft_ms) / 1000.0)
    try:
        try:
            result = handle(
                req.get("task"), req.get("args") or [], req.get("kwargs") or {}
            )
            json.dumps(result)  # serializability check
            out = {"id": rid, "ok": True, "result": result}
        finally:
            if timer is not None:
                armed[0] = False
                timer.cancel()
            elif soft_ms:
                signal.setitimer(signal.ITIMER_REAL, 0.0)
    except BaseException as e:
        out = {
            "id": rid,
            "ok": False,
            "error": {"type": type(e).__name__, "message": str(e), "traceback": ""},
        }
    return out


def stdio_main():
    signal.signal(signal.SIGALRM, _on_alarm)
    sys.stdout.write(json.dumps({"ready": True, "pid": os.getpid()}) + "\n")
    sys.stdout.flush()
    for line in sys.stdin:
        line = line.strip()
        if not line:
            continue
        try:
            req = json.loads(line)
        except Exception:
            continue
        out = run_request(req, threaded=False)
        sys.stdout.write(json.dumps(out) + "\n")
        sys.stdout.flush()


# --------------------------------------------------------------------------
# fork-server mode
# --------------------------------------------------------------------------


def _set_pdeathsig():
    try:
        libc = ctypes.CDLL(None, use_errno=True)
        libc.prctl(1, signal.SIGKILL, 0, 0, 0)  # PR_SET_PDEATHSIG == 1
    except Exception:
        pass


def _reap(signum, frame):
    while True:
        try:
            pid, _ = os.waitpid(-1, os.WNOHANG)
        except ChildProcessError:
            return
        if pid == 0:
            return


def child_main(sock_path, threads):
    _set_pdeathsig()
    signal.signal(signal.SIGCHLD, signal.SIG_DFL)
    sock = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
    sock.connect(sock_path)
    lock = threading.Lock()

    def send(payload):
        data = (json.dumps(payload) + "\n").encode()
        with lock:
            sock.sendall(data)

    send({"ready": True, "pid": os.getpid(), "concurrency": threads})
    rfile = sock.makefile("r", encoding="utf-8", errors="replace", newline="\n")

    if threads <= 1:
        signal.signal(signal.SIGALRM, _on_alarm)
        for line in rfile:
            line = line.strip()
            if not line:
                continue
            req = json.loads(line)
            if req.get("task") == "fx.cpu_ghost":
                # Audit regression fixture (worker/src/cpu.rs
                # serve_child_conn's "response with unknown or missing id"):
                # send one unsolicited line whose id never matches a pending
                # request, carrying a marker string in place of real task
                # output, before the real response. Lets an e2e test confirm
                # the worker's log line for that branch does not include
                # response payload content. Only reachable with the fork
                # server pool: the stdio fallback path matches no id at all.
                send(
                    {
                        "id": "ffffffffffffffffffffffffffffffff",
                        "ok": True,
                        "result": "GHOST_SECRET_MARKER",
                    }
                )
            send(run_request(req, threaded=False))
        return 0

    requests = queue.Queue()

    def worker():
        while True:
            req = requests.get()
            send(run_request(req, threaded=True))

    for _ in range(threads):
        threading.Thread(target=worker, daemon=True).start()
    for line in rfile:
        line = line.strip()
        if line:
            requests.put(json.loads(line))
    return 0


def fork_server_main(sock_path, threads):
    _set_pdeathsig()
    signal.signal(signal.SIGCHLD, _reap)
    sys.stdout.write(json.dumps({"server": True, "pid": os.getpid()}) + "\n")
    sys.stdout.flush()
    for line in sys.stdin:
        line = line.strip()
        if not line:
            continue
        try:
            cmd = json.loads(line)
        except Exception:
            continue
        if cmd.get("cmd") == "fork":
            pid = os.fork()
            if pid == 0:
                code = 1
                try:
                    code = child_main(sock_path, threads)
                except (BrokenPipeError, ConnectionResetError):
                    code = 0
                finally:
                    os._exit(code)
            sys.stdout.write(json.dumps({"forked": pid}) + "\n")
            sys.stdout.flush()
    return 0


def _argv_value(flag, default=None):
    if flag in sys.argv:
        i = sys.argv.index(flag)
        if i + 1 < len(sys.argv):
            return sys.argv[i + 1]
    return default


def main():
    # tolerate --app and any other argv
    if "--fork-server" in sys.argv:
        sock_path = _argv_value("--connect")
        threads = max(1, int(_argv_value("--child-threads", "1")))
        raise SystemExit(fork_server_main(sock_path, threads))
    stdio_main()


if __name__ == "__main__":
    main()
