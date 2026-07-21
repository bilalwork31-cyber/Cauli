"""rupy: Python client for the rupy Rust background worker runtime.

Define tasks with ``@app.task(...)``, enqueue with ``.delay()`` /
``.apply_async()``, read results with ``AsyncResult``. The heavy lifting
(execution, retries, timeouts) happens in the Rust ``rupy-worker`` process.
See PROTOCOL.md for the full wire contract.
"""

from rupy.app import Rupy
from rupy.exceptions import Retry, SoftTimeLimitExceeded, TaskFailedError
from rupy.result import AsyncResult
from rupy.task import TaskDef

__version__ = "0.1.0"

__all__ = [
    "Rupy",
    "Retry",
    "SoftTimeLimitExceeded",
    "TaskFailedError",
    "AsyncResult",
    "TaskDef",
    "__version__",
]
