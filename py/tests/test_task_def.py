"""Test 7: is_async detection, kind validation, name defaults, unit conversions."""

from __future__ import annotations

import importlib.machinery
import sys
import types

import pytest

from cauli import Cauli, TaskDef


def _offline_app() -> Cauli:
    return Cauli(redis_url="redis://127.0.0.1:1/0")


def test_is_async_detection():
    app = _offline_app()

    @app.task()
    def sync_task():
        return 1

    @app.task()
    async def async_task():
        return 1

    assert sync_task.is_async is False
    assert async_task.is_async is True


def test_kind_defaults_and_validation():
    app = _offline_app()

    @app.task()
    def default_kind():
        return None

    @app.task(kind="io")
    def io_kind():
        return None

    @app.task(kind="cpu")
    def cpu_kind():
        return None

    assert default_kind.kind == "io"
    assert io_kind.kind == "io"
    assert cpu_kind.kind == "cpu"

    with pytest.raises(ValueError):

        @app.task(kind="gpu")
        def bad_kind():
            return None

    assert not any(name.endswith("bad_kind") for name in app._tasks)


def test_default_name_and_registration():
    app = _offline_app()

    @app.task()
    def foo():
        return None

    assert foo.name == f"{foo.fn.__module__}.{foo.fn.__qualname__}"
    assert app._tasks[foo.name] is foo
    assert isinstance(foo, TaskDef)


def test_duplicate_task_name_raises_and_keeps_the_first_registration():
    app = _offline_app()

    @app.task(name="jobs.run")
    def first():
        return 1

    with pytest.raises(ValueError, match="jobs.run"):

        @app.task(name="jobs.run")
        def second():
            return 2

    # The registry must still point at the FIRST function: silently
    # overwriting it is exactly the bug (the worker would then run a
    # different function body than the one `.delay()` looks callable for).
    assert app._tasks["jobs.run"] is first
    assert first() == 1


def test_bare_decorator_form():
    app = _offline_app()

    @app.task
    def bare():
        return 41

    assert isinstance(bare, TaskDef)
    assert bare() == 41
    assert bare.kind == "io"


def test_seconds_to_ms_conversions():
    app = _offline_app()

    @app.task(timeout=1.5, soft_timeout=0.25, backoff_base=0.1, backoff_max=2.5)
    def t():
        return None

    assert t.timeout_ms == 1500
    assert t.soft_timeout_ms == 250
    assert t.backoff_base_ms == 100
    assert t.backoff_max_ms == 2500


def test_soft_timeout_must_be_less_than_timeout():
    app = _offline_app()

    with pytest.raises(ValueError):

        @app.task(timeout=5, soft_timeout=5)
        def equal_limits():
            return None


def _fake_main(monkeypatch, *, file=None, spec_name=None):
    """Stand in for the __main__ module of a process started various ways."""
    main = types.ModuleType("__main__")
    main.__spec__ = (
        None if spec_name is None else importlib.machinery.ModuleSpec(spec_name, None)
    )
    if file is not None:
        main.__file__ = file
    monkeypatch.setitem(sys.modules, "__main__", main)


def _main_fn():
    def hello(x):
        return x

    hello.__module__ = "__main__"
    hello.__qualname__ = "hello"  # as if defined at module level, not nested
    return hello


def test_script_run_task_name_uses_the_importable_module_not_dunder_main(monkeypatch):
    """`python tasks.py` must not mint `__main__.hello`.

    The worker imports the same file by module name, so its registry is keyed
    `tasks.hello`; an envelope stamped `__main__.hello` misses that registry
    and is terminally dead lettered on the very first enqueue.
    """
    _fake_main(monkeypatch, file="/srv/app/tasks.py")
    t = _offline_app().task()(_main_fn())
    assert t.name == "tasks.hello"


def test_dash_m_run_task_name_prefers_the_dotted_spec_name(monkeypatch):
    """`python -m pkg.tasks` knows its own dotted name; the stem would lose
    the package and mis-key the worker registry."""
    _fake_main(monkeypatch, file="/srv/app/pkg/tasks.py", spec_name="pkg.tasks")
    t = _offline_app().task()(_main_fn())
    assert t.name == "pkg.tasks.hello"


def test_unresolvable_main_module_warns_instead_of_minting_a_dead_name(monkeypatch):
    """A REPL or `python -c` has no importable name to recover, so the only
    honest option is to say so loudly rather than fail silently at enqueue."""
    _fake_main(monkeypatch)  # no __file__, no spec
    with pytest.warns(RuntimeWarning, match="dead lettered"):
        t = _offline_app().task()(_main_fn())
    assert t.name == "__main__.hello"


def test_explicit_name_skips_the_main_module_fixup_entirely(monkeypatch):
    _fake_main(monkeypatch)
    t = _offline_app().task(name="jobs.hello")(_main_fn())
    assert t.name == "jobs.hello"
