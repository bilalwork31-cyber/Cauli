"""cauli: Python client for the cauli Rust background worker runtime.

Define tasks with ``@app.task(...)``, enqueue with ``.delay()`` /
``.apply_async()``, read results with ``AsyncResult``. The heavy lifting
(execution, retries, timeouts) happens in the Rust ``cauli-worker`` process.
See PROTOCOL.md for the full wire contract.
"""

from cauli._context import TaskContext, current_task
from cauli.app import AsyncCauli, Cauli
from cauli.exceptions import Retry, SoftTimeLimitExceeded, TaskFailedError
from cauli.result import AsyncResult
from cauli.schedules import (
    CrontabSchedule,
    IntervalSchedule,
    Schedule,
    ScheduleEntry,
    crontab,
    interval,
)
from cauli.task import TaskDef

__version__ = "1.0.0"

__all__ = [
    "Cauli",
    "AsyncCauli",
    "Retry",
    "SoftTimeLimitExceeded",
    "TaskFailedError",
    "AsyncResult",
    "TaskDef",
    "TaskContext",
    "current_task",
    "Schedule",
    "ScheduleEntry",
    "IntervalSchedule",
    "CrontabSchedule",
    "crontab",
    "interval",
    "__version__",
]
