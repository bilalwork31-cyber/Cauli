# Decision: packaging, distribution and the first fifteen minutes
> **Historical design note, not current documentation.** This is a record of how one
> pre 1.0 decision was reached and what was known when it was reached. It is kept
> because the reasoning is worth reading, not because it describes today's behaviour.
> Where it disagrees with the code, with [PROTOCOL.md](../../PROTOCOL.md) or with
> [docs/CONFIGURATION.md](../CONFIGURATION.md), those win. The status line below was
> checked against the source, not carried over.
>
> **Status: shipped in 1.0.0.** Wheels are built for CPython 3.10 through 3.14, and the
> `cauli-worker` console script points the dynamic loader at the running interpreter's own
> library directory before it execs the binary, which is what makes conda, uv managed
> interpreters and minimal containers work. Claiming the PyPI names happens outside this
> tree and is not something the repository can show.

**The release pipeline is genuinely well built. Three traps sit in a stranger's first fifteen minutes,
and two of them block 1.0.**

## What is already good, and should be kept

`release.yml` builds a maturin `bindings = "bin"` wheel per CPython minor per arch, cp310 to cp313,
x86_64 and aarch64, manylinux_2_28. It gates dynamic linking with readelf on every wheel, verifies in
a clean venv against a real redis itest, publishes via OIDC trusted publishing, and asserts that the
installed `cauli-worker --version` equals `cauli.__version__`. `scripts/check_versions.py` gates four
locations plus the tag on every push, and the worker wheel pins `cauli==0.1.0` exactly. That lockstep
machinery is in better shape than most 1.0s and none of it blocks.

## Blocker: no Python 3.14 wheels

`PYTHONS` in release.yml covers cp310 to cp313. pyo3 0.26 supports 3.14. **Without adding it, the
current default Python cannot install the worker at all.** Add cp314 to the release matrix, the CI
matrix and both classifier lists.

## Blocker: a libpython loader failure that CI structurally cannot see

The binary carries `NEEDED: libpython3.X.so.1.0` and has **no RUNPATH**, since there is no build.rs
and no cargo config. The loader can therefore only find libpython through ldconfig or
`LD_LIBRARY_PATH`. When it cannot, the failure is pre main, so even `cauli-worker --version` dies with
`error while loading shared libraries`.

**CI is blind to this because setup-python sets `LD_LIBRARY_PATH` itself.** Every green run is masked.

Verified empirically on Ubuntu 24.04: the shared object lives in the `libpython3.12t64` package, which
`python3.12` does NOT depend on. It was present on the test machine only because vim and python3-dev
pull it in.

| environment | install | first run |
|-------------|---------|-----------|
| Docker `python:X` images | ok | ok, ldconfig'd |
| GitHub setup-python | ok | ok, and this is why CI is blind |
| desktop distro with python3-dev, vim or gdb | ok | ok |
| minimal ubuntu or debian container | ok | **loader error** |
| uv managed Python | ok | **loader error** |
| conda | ok | **loader error**, and the README wrongly lists conda as qualifying |
| pyenv default | ok | **loader error**, README does warn |
| Alpine or musl, glibc below 2.28, macOS, Windows | pip rejects | clear and pre deploy |

Recommended fix, which closes two findings at once: a small Python entry point wrapper that sets
`LD_LIBRARY_PATH` from `sysconfig.get_config_var("LIBDIR")` and `VIRTUAL_ENV` from `sys.prefix`, then
execs the binary.

## Blocker: PyPI names are not claimed

Both `cauli` and `cauli-worker` return 404. The names are free, but the publish job fails without
pending trusted publishers configured and a `pypi` environment in repo settings.

## The second trap: app import fails with no hint

`shim.py:173` resolves the app module only through cwd plus `VIRTUAL_ENV`, which
`docs/CONFIGURATION.md:275` requires. An activated shell works. A systemd unit or a Dockerfile CMD
using an absolute path gives `ModuleNotFoundError: No module named 'myproj'` with zero indication
that `VIRTUAL_ENV` is the cause. It should detect `pyvenv.cfg` beside its own binary, or at minimum
append "is VIRTUAL_ENV set?" to that error.

## Platform and documentation corrections

Linux only is real and enforced by construction: `libc::prctl(PR_SET_PDEATHSIG)` is unconditional in
both cpu.rs and supervisor.rs and will not compile elsewhere. But README line 41, "building from
source has no such constraint", reads as cross platform when it only means the glibc constraint.
Source builds are still Linux only. The glibc 2.28 claim is accurate. Alpine users learn at install
time from pip, which is acceptable, but the README never says musl requires a source build.

Version coupling gap worth noting: the dead letter reason for an unsupported protocol version is
`malformed`, so the client sees a misleading cause; and nothing at startup verifies the installed
`cauli` package against the binary, which matters for tarball deployments that bypass pip.

## Blocks tagging today

Adding cp314, the loader fix, claiming the PyPI names, and the version bump with a publish disabled
dry run. Strongly advised but not blocking: a CI leg that does NOT inherit setup-python's
`LD_LIBRARY_PATH`, using a minimal `ubuntu:24.04` container and a uv venv, plus one aarch64 execution
smoke test, since arm wheels currently ship on readelf alone.
