# cauli-worker

The Rust worker runtime for [cauli](https://pypi.org/project/cauli/), a background task queue
for Python. One binary embeds CPython (via PyO3) and executes thousands of tasks concurrently
inside a single OS process: async/threaded I/O tasks in-process, CPU-bound tasks on a
`gc.freeze()`d fork-server child pool.

See the repository root `README.md` for the overall pitch and Python-side quickstart, and
`PROTOCOL.md` for the full wire/behavior contract this binary implements. `ARCHITECTURE.md` in
this directory maps the module layout and documents known limitations.

## Build

```bash
cargo build --release --bin cauli-worker
```

## Run

```bash
cauli-worker --app myproj.tasks:app --queues default,emails \
    --redis-url redis://127.0.0.1:6379/0 --cpu-workers 4
```

Requires Redis >= 7.0 and a Python interpreter with the `cauli` package's app module
importable. Run `cauli-worker --help` for the full flag list.

## Test

```bash
cargo test --release --features test-hooks
```

`test-hooks` gates the `CAULI_EXEC_CMD` env override used by the e2e suite to run a
stand-in cpu-child fixture without the real `cauli` Python package; it is compiled out of a
plain `cargo build --release`.

## License

Licensed under either of Apache-2.0 or MIT at your option. See `LICENSE-APACHE` and
`LICENSE-MIT` at the repository root.
