# Packaging and distribution of the worker

Status: Accepted (0.1.0)

## Context

The worker embeds CPython through pyo3, so its binary carries
`NEEDED: libpython3.X.so.1.0` and runs only against that CPython minor version.
It has no build.rs and no cargo link arguments, so it has no RUNPATH, and the
dynamic loader can find libpython only through the ldconfig cache or
`LD_LIBRARY_PATH`. When neither applies, the binary fails inside the loader
before `main`, and even `cauli-worker --version` prints `error while loading
shared libraries`.

That is the case for a uv managed interpreter, a minimal Debian or Ubuntu
container, most pyenv builds and conda. On Ubuntu 24.04 the shared library is in
the `libpython3.12t64` package, which `python3.12` does not depend on. CI did not
see the failure because `actions/setup-python` exports `LD_LIBRARY_PATH` itself.

A second failure appeared in the same deployments: the shim finds the user's app
through the working directory and `VIRTUAL_ENV`. A systemd unit or a Dockerfile
`CMD` that runs the venv's binary by absolute path never activates the venv, and
the app import fails with a bare `ModuleNotFoundError`.

## Decision

- Publish one `cauli-worker` wheel per CPython minor version, 3.10 through 3.14,
  for x86_64 and aarch64, built with maturin `bindings = "bin"` on
  `ubuntu-22.04` runners. The wheels are tagged by the glibc the binary needs,
  which makes the floor glibc 2.35. manylinux_2_28 containers are not used: they
  ship a CPython without a shared libpython, and pyo3's `auto-initialize`
  refuses to build against it.
- Install a Python console script as `cauli-worker`
  (`worker/wheel-data/scripts/cauli-worker`) and the real binary beside it as
  `cauli-worker-bin`. The script appends the running interpreter's libpython
  directory to `LD_LIBRARY_PATH`, sets `VIRTUAL_ENV` from `sys.prefix` when it is
  unset and the interpreter is a venv, then calls `os.execv` on the binary.
  Both edits only add to the environment, so a setup that already works is
  unchanged. `execv` rather than a subprocess keeps the process tree intact,
  which `PR_SET_PDEATHSIG` on the cpu children and the supervisor depend on.
- Keep the two packages in lockstep. `cauli-worker` pins `cauli==<version>`,
  `cauli` pins `cauli-worker==<version>` behind a Linux, CPython, x86_64 or
  aarch64, `python_version < '3.15'` marker, and `scripts/check_versions.py`
  checks every version location (and the tag, at release) on every push.
- In `.github/workflows/release.yml`, gate every wheel on `readelf` showing
  dynamic libpython linkage, install it into a clean venv and run the Redis
  integration suite (`verify`), assert that `cauli-worker --version` equals
  `cauli.__version__`, and run `verify-bare`: a uv venv inside a plain
  `ubuntu:24.04` container with no `setup-python`, which reproduces the loader
  failure if the wrapper regresses. Publish through PyPI trusted publishing
  (OIDC) from the `pypi` environment.

## Consequences

- The worker is Linux only. `PR_SET_PDEATHSIG` is called unconditionally in
  `worker/src/cpu.rs` and `worker/src/supervisor.rs`, so source builds are Linux
  only too.
- musl (Alpine), glibc below 2.35, macOS and Windows get no wheel and pip refuses
  the install. The free threaded build has no wheel. The README lists conda as
  unsupported.
- An interpreter built without `--enable-shared` has no libpython to point at,
  and the worker cannot run on it.
- Running `cauli-worker-bin` directly skips the wrapper and needs the loader
  path set by hand.
- `verify` and `verify-bare` run x86_64 wheels only. aarch64 wheels are checked
  with `readelf` but never executed in CI.
- An envelope with a protocol version newer than the worker supports is dead
  lettered with reason `malformed`, which does not name the cause. Nothing at
  startup checks the installed `cauli` package against the binary, which
  matters only for deployments that bypass pip.

## Alternatives considered

- Have the shim detect `pyvenv.cfg` beside its own binary, or append "is
  VIRTUAL_ENV set?" to the import error. Superseded by the wrapper, which fixes
  both the loader path and `VIRTUAL_ENV` in one place.
