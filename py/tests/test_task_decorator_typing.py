"""``@app.task`` is typed by two overloads, so a py.typed consumer does not get
``TaskDef | Callable`` (on which ``.delay()`` does not resolve) for every task.

Typing-only: the runtime assertions below exist to keep the overloads from
drifting away from the implementation they describe.
"""

from __future__ import annotations

import inspect
import sys
import typing

import pytest

from cauli import Cauli
from cauli.task import TaskDef

needs_get_overloads = pytest.mark.skipif(
    sys.version_info < (3, 11), reason="typing.get_overloads is 3.11+"
)


def test_both_decorator_forms_still_return_a_taskdef_at_runtime(app):
    @app.task
    def bare():
        return 1

    @app.task(name="called")
    def called():
        return 2

    assert isinstance(bare, TaskDef)
    assert isinstance(called, TaskDef)
    assert bare() == 1 and called() == 2


@needs_get_overloads
def test_task_declares_exactly_two_overloads():
    overloads = typing.get_overloads(Cauli.task)
    assert len(overloads) == 2

    bare, keyworded = overloads
    assert bare.__annotations__["return"] == "TaskDef"
    assert (
        keyworded.__annotations__["return"] == "Callable[[Callable[..., Any]], TaskDef]"
    )


@needs_get_overloads
def test_the_keyword_overload_lists_every_implementation_option():
    keyworded = typing.get_overloads(Cauli.task)[1]
    assert list(inspect.signature(keyworded).parameters) == list(
        inspect.signature(Cauli.task).parameters
    ), "the overload and the implementation must accept the same keywords"
