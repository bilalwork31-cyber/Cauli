# Contributing

Open an issue before anything larger than a bug fix or a documentation change.
The non goals in [PROTOCOL.md section 11](PROTOCOL.md#11-non-goals-for-v1) are
declined however well implemented: chains, groups, chords, rate limits,
priorities, brokers other than Redis, Redis Cluster.

Wanted: a `cauli` CLI for the dead letter queue, queue depth and the delayed
set; the benchmark lanes [bench/RESULTS.md](bench/RESULTS.md) lists as not yet
done; issues labelled `good first issue`.

## Repository layout

| Path | What it is |
|---|---|
| `worker/` | the Rust worker, including the embedded Python shim at `worker/src/shim.py` |
| `py/` | the `cauli` client library and `cauli-beat` |
| `itest/` | cross component tests: a real worker binary against a real Redis |
| `bench/` | the benchmark campaign. Read `bench/CLAIMS.md` before any number in it |
| `docs/` | configuration, architecture and design decisions, Celery migration |

## Running the checks

| Platform | What you can build and test |
|---|---|
| Linux | everything below |
| macOS | the Python client suite and the linters. The worker does not build |
| Windows | use WSL 2 (Ubuntu) and follow the Linux steps. Native Windows runs only the linters and the tests that need no Redis |

The worker needs a CPython built with `--enable-shared` (python.org, uv,
distro packages; pyenv only with `PYTHON_CONFIGURE_OPTS=--enable-shared`).
The suites need `redis-server` on `PATH`, not a running Redis: each suite
starts its own throwaway instances on ports 6390 to 6399, so stop anything
else using those ports first. `scripts/check_versions.py` needs Python 3.11 or
newer.

```bash
# Rust: the test-hooks feature is required, several tests do not exist without it
cd worker
cargo fmt --check
cargo clippy --all-targets -- -D warnings
cargo clippy --all-targets --features test-hooks -- -D warnings
# With no virtualenv activated: the tests embed CPython, which finds an
# activated venv through PATH, and one test needs cauli NOT to be importable.
# The worker decodes task args with msgspec, so it must be importable:
#   pip install --target ~/msgspec-site msgspec && export PYTHONPATH=~/msgspec-site
cargo test --release --features test-hooks

# Python client
pip install -e "py[dev]"   # includes the pinned ruff; a newer ruff reports errors CI does not
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

`itest/test_django.py` also needs `django>=4.2`, `psycopg[binary]` and the
Postgres server binaries (`initdb`, `pg_ctl`); it skips itself without them.
On a slow or network filesystem, set `CARGO_TARGET_DIR` to a local path.

CI runs exactly these, plus the Python suite on 3.10 through 3.14 and a
packaging job that builds both distributions.

## What a change has to carry

- **A failing test before, a passing test after.** Reproduce, root cause, fix,
  verify. A fix with no test that would have caught it will be asked for one.
- **The wire format is stable.** Any change to the envelope, the Redis
  key names or the stats line key set is a breaking change and must be
  called out as one in the changelog. Adding a stats
  key is allowed; renaming or removing one is not.
  [PROTOCOL.md](PROTOCOL.md) is the contract, and a protocol change means
  changing that document in the same pull request.
- **Documentation lands with the behaviour.** If a flag, a default or a failure
  mode changes, [docs/configuration.md](docs/configuration.md) and
  [CHANGELOG.md](CHANGELOG.md) change in the same commit.
- **No number without a harness.** A performance figure in any document must
  be reproducible by something in `bench/`.

## Reporting a bug

Include the version of both packages, the Redis version, whether the worker is
the published wheel or a source build, your worker command line or the output
of `--print-plan`, and the log around the failure. The stats line is usually
the fastest thing to read, so paste one.

Security issues go through [SECURITY.md](SECURITY.md), not the issue tracker.

## License

Contributions are accepted under the same dual license as the project,
Apache-2.0 or MIT at the user's option. There is no CLA.
