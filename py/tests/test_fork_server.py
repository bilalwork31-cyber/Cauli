"""cauli._exec fork-server mode (PROTOCOL.md 5.1): control channel fork,
socket ready line, threaded pipelining, EOF shutdown, gc.freeze inheritance.

The test plays the Rust worker: it owns the unix socket listener and the
parent's stdin/stdout control channel. Linux/WSL only (fork + AF_UNIX)."""

from __future__ import annotations

import json
import os
import socket
import subprocess
import sys
import tempfile
import time
from pathlib import Path

import pytest

TESTS_DIR = Path(__file__).resolve().parent

pytestmark = pytest.mark.skipif(
    not hasattr(os, "fork") or not hasattr(socket, "AF_UNIX"),
    reason="fork-server mode needs os.fork() and AF_UNIX sockets",
)


class ChildConn:
    """One forked child's socket connection, driven line by line."""

    def __init__(self, sock: socket.socket) -> None:
        self.sock = sock
        self.rfile = sock.makefile("r", encoding="utf-8", newline="\n")
        self.ready = json.loads(self._readline())

    def _readline(self, timeout: float = 15.0) -> str:
        self.sock.settimeout(timeout)
        line = self.rfile.readline()
        if line == "":
            raise AssertionError("child connection EOF")
        return line

    def send(self, payload: dict) -> None:
        self.sock.sendall((json.dumps(payload) + "\n").encode())

    def recv(self, timeout: float = 15.0) -> dict:
        return json.loads(self._readline(timeout))

    def request(self, payload: dict, timeout: float = 15.0) -> dict:
        self.send(payload)
        return self.recv(timeout)

    def at_eof(self, timeout: float = 10.0) -> bool:
        self.sock.settimeout(timeout)
        try:
            return self.rfile.readline() == ""
        except OSError:
            return True

    def close(self) -> None:
        try:
            self.rfile.close()
        except Exception:
            pass
        self.sock.close()


class ForkServer:
    """Drives one `python -m cauli._exec --fork-server` parent (as the worker)."""

    def __init__(self, child_threads: int = 1) -> None:
        self.dir = tempfile.mkdtemp(prefix="cauli-fs-")
        self.sock_path = os.path.join(self.dir, "cpu.sock")
        self.listener = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
        self.listener.bind(self.sock_path)
        self.listener.listen(8)
        self.stderr_file = open(os.path.join(self.dir, "stderr.log"), "wb")
        self.proc = subprocess.Popen(
            [
                sys.executable,
                "-m",
                "cauli._exec",
                "--app",
                "exec_fixture_app:app",
                "--fork-server",
                "--connect",
                self.sock_path,
                "--child-threads",
                str(child_threads),
            ],
            cwd=str(TESTS_DIR),
            stdin=subprocess.PIPE,
            stdout=subprocess.PIPE,
            stderr=self.stderr_file,
            encoding="utf-8",
        )
        self.server_line = json.loads(self._read_control())
        self.conns: list[ChildConn] = []

    def _read_control(self, timeout: float = 20.0) -> str:
        # The parent writes one line per control command; a blocking readline
        # with a deadline via poll on the fd.
        import select

        assert self.proc.stdout is not None
        deadline = time.monotonic() + timeout
        while time.monotonic() < deadline:
            r, _, _ = select.select([self.proc.stdout], [], [], 0.1)
            if r:
                line = self.proc.stdout.readline()
                if line == "":
                    raise AssertionError(
                        f"control channel EOF; stderr: {self.stderr()!r}"
                    )
                return line
        raise AssertionError(f"control read timeout; stderr: {self.stderr()!r}")

    def fork(self) -> int:
        assert self.proc.stdin is not None
        self.proc.stdin.write('{"cmd":"fork"}\n')
        self.proc.stdin.flush()
        reply = json.loads(self._read_control())
        assert "forked" in reply, f"unexpected fork reply: {reply}"
        return reply["forked"]

    def accept_child(self, timeout: float = 15.0) -> ChildConn:
        self.listener.settimeout(timeout)
        sock, _ = self.listener.accept()
        conn = ChildConn(sock)
        self.conns.append(conn)
        return conn

    def control(self, payload: dict) -> dict:
        assert self.proc.stdin is not None
        self.proc.stdin.write(json.dumps(payload) + "\n")
        self.proc.stdin.flush()
        return json.loads(self._read_control())

    def close_stdin(self) -> None:
        assert self.proc.stdin is not None
        self.proc.stdin.close()

    def stderr(self) -> str:
        self.stderr_file.flush()
        try:
            with open(self.stderr_file.name, "rb") as f:
                return f.read().decode("utf-8", "replace")
        except Exception:
            return "<unreadable>"

    def shutdown(self) -> None:
        for conn in self.conns:
            conn.close()
        if self.proc.poll() is None:
            self.proc.kill()
            self.proc.wait(timeout=5)
        for stream in (self.proc.stdin, self.proc.stdout):
            try:
                if stream:
                    stream.close()
            except Exception:
                pass
        self.stderr_file.close()
        self.listener.close()
        try:
            os.remove(self.sock_path)
            os.rmdir(self.dir)
        except OSError:
            pass


@pytest.fixture()
def fs():
    server = ForkServer(child_threads=1)
    assert server.server_line == {"server": True, "pid": server.proc.pid}
    yield server
    server.shutdown()


@pytest.fixture()
def fs_threaded():
    server = ForkServer(child_threads=3)
    assert server.server_line == {"server": True, "pid": server.proc.pid}
    yield server
    server.shutdown()


def _wait_pid_gone(pid: int, timeout: float = 10.0) -> bool:
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        try:
            os.kill(pid, 0)
        except ProcessLookupError:
            return True
        time.sleep(0.05)
    return False


def test_control_fork_and_socket_ready_line(fs):
    pid = fs.fork()
    assert pid != fs.proc.pid
    child = fs.accept_child()
    assert child.ready == {"ready": True, "pid": pid, "concurrency": 1}

    resp = child.request(
        {
            "id": "r1",
            "task": "add",
            "args": [2, 3],
            "kwargs": {},
            "soft_timeout_ms": None,
        }
    )
    assert resp == {"id": "r1", "ok": True, "result": 5}


def test_children_share_frozen_parent_image(fs):
    # Two children: distinct pids, both inherit a frozen (> 0) permanent
    # generation from the parent's post-import gc.freeze().
    pid1, pid2 = fs.fork(), fs.fork()
    c1, c2 = fs.accept_child(), fs.accept_child()
    assert pid1 != pid2
    assert {c1.ready["pid"], c2.ready["pid"]} == {pid1, pid2}

    for c in (c1, c2):
        resp = c.request(
            {
                "id": "f",
                "task": "freeze_count",
                "args": [],
                "kwargs": {},
                "soft_timeout_ms": None,
            }
        )
        assert resp["ok"] is True
        assert resp["result"] > 0, "gc.freeze() must be inherited by the child"


def test_task_errors_and_retry_still_work_over_socket(fs):
    fs.fork()
    child = fs.accept_child()
    resp = child.request(
        {"id": "e1", "task": "boom", "args": [], "kwargs": {}, "soft_timeout_ms": None}
    )
    assert resp["ok"] is False
    assert resp["error"]["type"] == "ValueError"

    resp = child.request(
        {
            "id": "t1",
            "task": "retryme",
            "args": [2.5],
            "kwargs": {},
            "soft_timeout_ms": None,
        }
    )
    assert resp["ok"] is False
    assert resp["retry"] is True
    assert resp["countdown"] == 2.5


def test_single_threaded_soft_timeout_sigalrm(fs):
    fs.fork()
    child = fs.accept_child()
    t0 = time.monotonic()
    resp = child.request(
        {
            "id": "s1",
            "task": "sleepy",
            "args": [5],
            "kwargs": {},
            "soft_timeout_ms": 200,
        },
        timeout=10,
    )
    assert time.monotonic() - t0 < 2.0
    assert resp["ok"] is False
    assert resp["error"]["type"] == "SoftTimeLimitExceeded"


def test_threaded_child_pipelines_and_matches_by_id(fs_threaded):
    pid = fs_threaded.fork()
    child = fs_threaded.accept_child()
    assert child.ready == {"ready": True, "pid": pid, "concurrency": 3}

    t0 = time.monotonic()
    for i in range(3):
        child.send(
            {
                "id": f"p{i}",
                "task": "sleepy",
                "args": [0.5],
                "kwargs": {},
                "soft_timeout_ms": None,
            }
        )
    got = {}
    for _ in range(3):
        resp = child.recv(timeout=10)
        got[resp["id"]] = resp
    elapsed = time.monotonic() - t0
    assert set(got) == {"p0", "p1", "p2"}
    assert all(r == {"id": i, "ok": True, "result": "done"} for i, r in got.items())
    assert elapsed < 1.2, f"3 x 0.5s must interleave on 3 threads, took {elapsed:.2f}s"

    # all three ran in ONE process (the child), on worker threads
    resp = child.request(
        {
            "id": "pi",
            "task": "pidinfo",
            "args": [],
            "kwargs": {},
            "soft_timeout_ms": None,
        }
    )
    assert resp["result"]["pid"] == pid


def test_threaded_soft_timeout_via_watchdog(fs_threaded):
    fs_threaded.fork()
    child = fs_threaded.accept_child()
    t0 = time.monotonic()
    resp = child.request(
        {
            "id": "w1",
            "task": "sleepy_slices",
            "args": [5],
            "kwargs": {},
            "soft_timeout_ms": 200,
        },
        timeout=10,
    )
    assert time.monotonic() - t0 < 2.0
    assert resp["ok"] is False
    assert resp["error"]["type"] == "SoftTimeLimitExceeded"

    # the watchdog disarmed cleanly: a longer, un-timed request completes
    resp = child.request(
        {
            "id": "w2",
            "task": "sleepy_slices",
            "args": [0.4],
            "kwargs": {},
            "soft_timeout_ms": None,
        }
    )
    assert resp == {"id": "w2", "ok": True, "result": "done"}


def test_unknown_control_command_is_reported(fs):
    reply = fs.control({"cmd": "selfdestruct"})
    assert "error" in reply
    # parent survives and still forks
    fs.fork()
    fs.accept_child()


def test_eof_shuts_down_parent_and_children(fs):
    pid = fs.fork()
    child = fs.accept_child()
    parent_pid = fs.proc.pid

    fs.close_stdin()
    assert fs.proc.wait(timeout=10) == 0, "parent must exit 0 on control EOF"
    # children die with the parent (PR_SET_PDEATHSIG) -> socket EOF, pid gone
    assert child.at_eof(timeout=10)
    assert _wait_pid_gone(pid), f"forked child {pid} outlived parent {parent_pid}"
