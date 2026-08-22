# Contributing

Thanks for looking. Read this first, it is short.

## Before you write code

Open an issue for anything larger than a bug fix or a documentation change.
This project has a narrow scope on purpose: chains, groups, chords, rate
limits, task priorities, brokers other than Redis and Redis Cluster are all
non goals for 1.x, listed in [PROTOCOL.md](PROTOCOL.md) section 11. A pull
request implementing one of those will be declined however good it is, and an
issue first saves you the work.

Good places to start, in rough order of value:

- A `cauli` CLI for the dead letter queue, queue depth and the delayed set.
  Nothing ships for any of it today, and CHANGELOG's upgrade notes tell
  operators to arrange to drain or export the dead letter queue with no tool to
  do it. Celery's `inspect`, `purge` and `control` have no equivalent either.
- Benchmark lanes `bench/RESULTS.md` names as never started, particularly the
  retry rate lane and the result round trip latency lane.
- The open items listed at the end of the "Pre release audit fixes" section in
  [CHANGELOG.md](CHANGELOG.md).

## Repository layout

| Path | What it is |
|---|---|
| `worker/` | the Rust worker, including the embedded Python shim at `worker/src/shim.py` |
| `py/` | the `cauli` client library and `cauli-beat` |
| `itest/` | cross component tests: a real worker binary against a real Redis |
| `bench/` | the benchmark campaign. Read `bench/CLAIMS.md` before any number in it |
| `docs/decisions/` | historical design notes, not current documentation |

## Running the checks

The worker needs Linux and a CPython built with `--enable-shared`. A local
`redis-server` on the default port is enough for the suites.

```bash
# Rust: the test-hooks feature is required, several tests do not exist without it
cd worker
cargo fmt --check
cargo clippy --all-targets -- -D warnings
cargo clippy --all-targets --features test-hooks -- -D warnings
cargo test --release --features test-hooks

# Python client
pip install -e "py[dev]"
pip install ruff==0.15.22
ruff check py/ worker/src/shim.py itest/ scripts/ worker/wheel-data/scripts/cauli-worker
ruff format --check py/ worker/src/shim.py itest/ scripts/ worker/wheel-data/scripts/cauli-worker
cd py && pytest -q

# Cross component, against a release worker you just built
cd worker && cargo build --release --bin cauli-worker
cd ../itest
CAULI_WORKER_BIN=$PWD/../worker/target/release/cauli-worker pytest -q

# Versions agree across the six places that carry one, README included
python scripts/check_versions.py
```

CI runs exactly these, plus the Python suite on 3.10 through 3.14 and a
packaging job that builds both distributions.

## What a change has to carry

- **A failing test before, a passing test after.** Reproduce, root cause, fix,
  verify. A fix with no test that would have caught it will be asked for one.
- **The wire format is frozen for 1.x.** Any change to the envelope, the Redis
  key names or the stats line key set is a major version change. Adding a stats
  key is allowed; renaming or removing one is not.
  [PROTOCOL.md](PROTOCOL.md) is the contract, and a protocol change means
  changing that document in the same pull request.
- **Documentation lands with the behaviour.** If a flag, a default or a failure
  mode changes, [docs/CONFIGURATION.md](docs/CONFIGURATION.md) and
  [CHANGELOG.md](CHANGELOG.md) change in the same commit.
- **No number without a harness.** Do not add a performance figure to any
  document unless something in `bench/` reproduces it. Every unbacked figure
  this repository once carried has been removed, and they are not coming back.

## Reporting a bug

Include the version of both packages, the Redis version, whether the worker is
the published wheel or a source build, your worker command line or the output
of `--print-plan`, and the log around the failure. The stats line is usually
the fastest thing to read, so paste one.

Security issues go through [SECURITY.md](SECURITY.md), not the issue tracker.

## License

Contributions are accepted under the same dual license as the project,
Apache-2.0 or MIT at the user's option. There is no CLA.
