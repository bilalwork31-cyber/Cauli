# cauli

Python client for cauli, a task queue whose worker is one Rust binary.
Documentation: https://github.com/bilalwork31-cyber/Cauli

## Install

    pip install cauli

This also installs the `cauli-worker` binary where its wheels exist: Linux on
x86_64 or aarch64, CPython 3.10 through 3.14. Elsewhere the worker requirement
is dropped and the client still installs and enqueues. musl (Alpine) and
free threaded 3.13t fail the install instead; see PROTOCOL.md section 13.4 for
the enqueue only install.

## Use

    from cauli import Cauli

    app = Cauli(redis_url="redis://localhost:6379/0")

    @app.task()
    async def send_email(to: str) -> None: ...

    r = send_email.delay("a@b.com")
    r.status()           # "pending" | "success" | "failure" | "duplicate" | "expired"
    r.get(timeout=10)

Run the worker with `cauli-worker -A myproj.tasks:app -c 50`.
