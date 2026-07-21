"""cauli: Python client for the cauli Rust background worker runtime.

Define tasks with ``@app.task(...)``, enqueue with ``.delay()`` /
``.apply_async()``, read results with ``AsyncResult``. The heavy lifting
(execution, retries, timeouts) happens in the Rust ``cauli-worker`` process.
See PROTOCOL.md for the full wire contract.
"""

from cauli.app import Cauli
from cauli.exceptions import Retry, SoftTimeLimitExceeded, TaskFailedError
from cauli.result import AsyncResult
from cauli.task import TaskDef

__version__ = "0.1.0"

__all__ = [
    "Cauli",
    "Retry",
    "SoftTimeLimitExceeded",
    "TaskFailedError",
    "AsyncResult",
    "TaskDef",
    "__version__",
]
