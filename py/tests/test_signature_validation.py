"""`.delay()`/`.apply_async()` validate args/kwargs against the task's own
signature and raise immediately, at the call site, instead of enqueuing a
call that can only ever fail once the worker invokes ``fn(*args, **kwargs)``.
"""

from __future__ import annotations

import functools

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
    legitimate call it has no safe way to verify.

    The introspection happens once, when the task is declared, rather than on
    every enqueue, so the patch has to be in place before the decorator runs.
    """

    def _uninspectable(_fn):
        raise ValueError("no signature found")

    monkeypatch.setattr(task_module.inspect, "signature", _uninspectable)

    @app.task()
    def add(a, b):
        return a + b

    assert add._signature is None
    add.delay(a=1, bee=2)  # cannot be checked, so this must NOT raise
    assert redis_client.xlen("cauli:q:default") == 1


def test_wraps_decorator_that_injects_an_argument_is_not_rejected(app, redis_client):
    # functools.wraps sets __wrapped__, and inspect.signature follows it to the
    # INNER function by default. The worker calls the wrapper, not the inner
    # function, so the check has to bind against what will actually be called.
    def inject_session(fn):
        @functools.wraps(fn)
        def wrapper(*args, **kwargs):
            return fn("session", *args, **kwargs)

        return wrapper

    @app.task()
    @inject_session
    def charge(session, order_id):
        return order_id

    assert charge(7) == 7
    charge.delay(7)
    assert redis_client.xlen("cauli:q:default") == 1
