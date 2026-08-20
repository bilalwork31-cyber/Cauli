"""`.delay()`/`.apply_async()` validate args/kwargs against the task's own
signature and raise immediately, at the call site, instead of enqueuing a
call that can only ever fail once the worker invokes ``fn(*args, **kwargs)``.
"""

from __future__ import annotations

import pytest

import cauli.task as task_module


def test_bad_keyword_argument_raises_immediately_and_never_enqueues(app, redis_client):
    @app.task()
    def add(a, b):
        return a + b

    with pytest.raises(TypeError, match="bee"):
        add.delay(1, 2, bee=3)
    assert redis_client.xlen("cauli:q:default") == 0, (
        "a call that fails signature validation must never reach the broker"
    )


def test_error_message_names_the_task(app):
    @app.task(name="math.add")
    def add(a, b):
        return a + b

    with pytest.raises(TypeError, match="math.add"):
        add.delay(a=1, bee=2)


def test_missing_required_argument_raises_immediately(app, redis_client):
    @app.task()
    def add(a, b):
        return a + b

    with pytest.raises(TypeError):
        add.delay(1)
    assert redis_client.xlen("cauli:q:default") == 0


def test_too_many_positional_arguments_raises_immediately(app, redis_client):
    @app.task()
    def add(a, b):
        return a + b

    with pytest.raises(TypeError):
        add.delay(1, 2, 3)


def test_valid_call_still_enqueues(app, redis_client):
    @app.task()
    def add(a, b):
        return a + b

    add.delay(1, b=2)  # must not raise
    assert redis_client.xlen("cauli:q:default") == 1


def test_star_args_and_kwargs_task_accepts_anything(app, redis_client):
    @app.task()
    def flexible(*args, **kwargs):
        return None

    flexible.delay(1, 2, x=3, y=4)  # must not raise
    assert redis_client.xlen("cauli:q:default") == 1


def test_apply_async_also_validates_task_signature(app, redis_client):
    @app.task()
    def add(a, b):
        return a + b

    with pytest.raises(TypeError, match="bee"):
        add.apply_async(kwargs={"a": 1, "b": 2, "bee": 3})
    assert redis_client.xlen("cauli:q:default") == 0


def test_apply_async_valid_call_still_enqueues(app, redis_client):
    @app.task()
    def add(a, b):
        return a + b

    add.apply_async(args=(1,), kwargs={"b": 2})  # must not raise
    assert redis_client.xlen("cauli:q:default") == 1


def test_signature_check_falls_through_when_uninspectable(
    app, redis_client, monkeypatch
):
    """A callable inspect.signature() cannot introspect must not block a
    legitimate call it has no safe way to verify."""

    @app.task()
    def add(a, b):
        return a + b

    def _uninspectable(_fn):
        raise ValueError("no signature found")

    monkeypatch.setattr(task_module.inspect, "signature", _uninspectable)
    add.delay(a=1, bee=2)  # cannot be checked, so this must NOT raise
    assert redis_client.xlen("cauli:q:default") == 1
