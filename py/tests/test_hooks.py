"""Lifecycle hooks (PROTOCOL.md section 4.8): registration API, execution
order and error isolation in the cpu child executor, and the stdio child's
process-init timing."""

from __future__ import annotations

from pathlib import Path

import pytest

from cauli import Cauli
from cauli import _exec
from cauli._hooks import run_hooks
from helpers import ExecChild

TESTS_DIR = Path(__file__).resolve().parent


def _app_no_redis() -> Cauli:
    return Cauli(redis_url="redis://127.0.0.1:1/0")


def test_registration_api_is_decorator_friendly_and_ordered():
    app = _app_no_redis()

    @app.before_task
    def b1():
        pass

    def b2():
        pass

    assert app.before_task(b2) is b2  # plain-call form returns the fn

    @app.after_task
    def a1():
        pass

    @app.process_init
    def p1():
        pass

    assert app._before_task_hooks == [b1, b2]
    assert app._after_task_hooks == [a1]
    assert app._process_init_hooks == [p1]
    assert b1.__name__ == "b1"  # decorator must not wrap/replace the fn


def test_run_hooks_isolates_exceptions_but_not_base_exceptions(capsys):
    calls = []

    def bad():
        raise ValueError("hook bug")

    run_hooks([bad, lambda: calls.append("ran")], "before_task")
    assert calls == ["ran"], "a raising hook must not stop later hooks"
    err = capsys.readouterr().err
    assert "before_task hook" in err and "ValueError" in err

    with pytest.raises(SystemExit):
        run_hooks([lambda: (_ for _ in ()).throw(SystemExit(3))], "before_task")


def test_execute_runs_hooks_around_success_and_failure():
    app = _app_no_redis()
    events = []

    @app.task(name="ok", kind="cpu")
    def ok():
        events.append("task")
        return 1

    @app.task(name="bad", kind="cpu")
    def bad():
        events.append("task")
        raise RuntimeError("nope")

    app.before_task(lambda: events.append("before"))
    app.after_task(lambda: events.append("after"))

    resp = _exec._execute(
        app,
        {"id": "1", "task": "ok", "args": [], "kwargs": {}, "soft_timeout_ms": None},
    )
    assert resp["ok"] is True
    assert events == ["before", "task", "after"]

    events.clear()
    resp = _exec._execute(
        app,
        {"id": "2", "task": "bad", "args": [], "kwargs": {}, "soft_timeout_ms": None},
    )
    assert resp["ok"] is False and resp["error"]["type"] == "RuntimeError"
    assert events == ["before", "task", "after"], "hooks must fire on failure too"


def test_execute_skips_hooks_for_unknown_task():
    app = _app_no_redis()
    events = []
    app.before_task(lambda: events.append("before"))
    app.after_task(lambda: events.append("after"))
    resp = _exec._execute(
        app,
        {"id": "3", "task": "nope", "args": [], "kwargs": {}, "soft_timeout_ms": None},
    )
    assert resp["error"]["type"] == "UnregisteredTask"
    assert resp["retryable"] is False
    assert events == [], "hooks wrap task execution, not registry misses"


def test_execute_survives_raising_hooks():
    app = _app_no_redis()

    @app.task(name="ok", kind="cpu")
    def ok():
        return "fine"

    def explode():
        raise OSError("hook down")

    app.before_task(explode)
    app.after_task(explode)
    resp = _exec._execute(
        app,
        {"id": "4", "task": "ok", "args": [], "kwargs": {}, "soft_timeout_ms": None},
    )
    assert resp == {"id": "4", "ok": True, "result": "fine"}


def test_stdio_child_runs_process_init_once_and_task_hooks_per_request(
    tmp_path, monkeypatch
):
    hooklog = tmp_path / "hooklog"
    monkeypatch.setenv("CAULI_TEST_HOOKLOG", str(hooklog))
    child = ExecChild(TESTS_DIR)
    try:
        ready = child.read_json(timeout=20)
        assert ready["ready"] is True
        # process_init must have run before the ready line (i.e. before any
        # task could possibly execute).
        phases = [line.split()[0] for line in hooklog.read_text().splitlines()]
        assert phases == ["process_init"]

        for i in range(2):
            resp = child.request(
                {
                    "id": f"h{i}",
                    "task": "add",
                    "args": [i, i],
                    "kwargs": {},
                    "soft_timeout_ms": None,
                }
            )
            assert resp["ok"] is True
        phases = [line.split()[0] for line in hooklog.read_text().splitlines()]
        assert phases == ["process_init", "before", "after", "before", "after"]
    finally:
        child.terminate()
