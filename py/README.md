# cauli (Python client)

Python client for **cauli**, a Rust background worker runtime for Python task queues.
Define tasks in Python, enqueue from any web framework, execute on the Rust worker.

## Install

    pip install cauli               # Python >= 3.10 (deps: redis>=5, msgspec)

That also installs the `cauli-worker` binary, but only where its wheels exist:
Linux (glibc) on x86_64 or aarch64, CPython 3.10 through 3.14. Anywhere else, a
macOS laptop, Windows, PyPy, a CPython newer than the release matrix, the worker
requirement is dropped rather than failing the install: the client still lands
and enqueueing still works, there is simply no `cauli-worker` to run. Two
targets fail the install instead, because no marker can describe them: musl
(Alpine) and the free threaded build. For those, and for enqueue only web dynos
that never run a worker, see `PROTOCOL.md` section 13.4.

## Define an app and tasks (myproj/tasks.py)

    from cauli import Cauli

    app = Cauli(redis_url="redis://localhost:6379/0", default_queue="default")

    @app.task()
    async def send_email(to: str) -> str:
        ...                      # io task: runs on the worker's asyncio loop
        return "sent"

    @app.task(kind="cpu", timeout=120, soft_timeout=60, max_retries=5)
    def crunch(n: int) -> int:   # cpu task: runs in a child Python process
        return sum(i * i for i in range(n))

## Enqueue from Django, FastAPI, or Flask

    from myproj.tasks import send_email

    r = send_email.delay("a@b.com")
    r = send_email.apply_async(args=("a@b.com",), countdown=30, queue="emails",
                               idempotency_key="welcome:a@b.com")
    r.status()           # "pending" | "success" | "failure" | "duplicate" | "expired"
    r.get(timeout=10)    # value on success; raises TaskFailedError / TimeoutError

Tasks stay directly callable for tests: `crunch(10)` runs inline, no broker needed.

## Run the worker (Rust binary)

    cauli-worker -A myproj.tasks:app -c 50 -Q default,emails

The URL falls back to env `CAULI_REDIS_URL`, then `redis://localhost:6379/0`.
See `PROTOCOL.md` in the repo root for the full wire contract.
