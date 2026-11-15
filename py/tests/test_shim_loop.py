"""The embedded asyncio loop thread outlives what a task raises into it."""

from __future__ import annotations

import asyncio
import threading

import pytest


@pytest.mark.parametrize("exc", [SystemExit(3), KeyboardInterrupt()])
def test_an_exit_raised_in_a_task_does_not_end_the_loop_thread(shim, exc, capsys):
    """asyncio re-raises SystemExit and KeyboardInterrupt out of run_forever.
    A dead loop thread fails every later async task and ends in exit 87."""
    loop = asyncio.new_event_loop()
    thread = threading.Thread(target=shim._loop_main, args=(loop,), daemon=True)
    thread.start()

    async def poison():
        raise exc

    async def healthy():
        return 7

    try:
        poisoned = asyncio.run_coroutine_threadsafe(poison(), loop)
        with pytest.raises(type(exc)):
            poisoned.result(timeout=5)
        assert asyncio.run_coroutine_threadsafe(healthy(), loop).result(timeout=5) == 7
        assert thread.is_alive()
    finally:
        loop.call_soon_threadsafe(loop.stop)
        thread.join(timeout=5)
    assert not thread.is_alive(), "a plain stop still ends the thread"
    assert type(exc).__name__ in capsys.readouterr().err
