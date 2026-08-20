# RESUME STATE — written 2026-08-17, laptop shutting down mid work

Branch `audit/overnight`, 56 commits. `main` untouched at `6256854`. Nothing here is on a remote.

## Read these first

- `docs/AUDIT_LOG.md`, about 3000 lines, the full record. Its header carries two tables: what needs
  human review before merge, and what needs a decision rather than a fix.
- `docs/decisions/`, nine design documents, each with a committed recommendation. All are marked NOT
  implemented unless a commit is named.
- `CHANGELOG.md`, 1.0.0, 21 breaking changes.

## Archive branches, do not delete without diffing

| branch | what |
|--------|------|
| `audit/stash-archive-58cc0f8` | commit 81c0e46, the stash that was live during the night, archived before being dropped. Almost certainly superseded, never verified line by line. |
| `audit/dangling-archive-f2e3151` | **a SECOND stash, found only because an agent noticed the ref had vanished.** Contains `py/cauli/_exec.py` +22/-2 and `worker/tests/e2e_forkserver.rs` +117. It was genuinely dangling and would have been lost to gc. Not verified against HEAD. **Diff this before deleting it.** |
| `backup/pre-fastapi-repair-393816d` | left from an earlier repair, untouched all night. |

## Work that was IN FLIGHT when the machine went down

Each of these was a subagent with its own context, now gone. The work is not resumable by messaging;
it has to be restarted from the decision document, which is why those documents exist.

| what | source of truth | state |
|------|----------------|-------|
| Observability field set: 6 latency fields, `oldest_ms`, `cpu_rss_mb`, `cpu_lost`, remove `pending_async`, recycle default to 1000, PROTOCOL section 7 | `docs/decisions/observability.md` | in progress, may have partial uncommitted edits in `worker/src/{stats,ctx,loops,cli,cpu,exec}.rs` |
| Packaging: cp314 wheels, the libpython loader wrapper, a CI leg that cannot mask it, version bump to 1.0.0 | `docs/decisions/packaging.md` | **partially committed by accident in 8ce73f3**, see below |
| Delivery guarantee: claim TTL derived from execution, claimant id in duplicate results, retry delay clamp, PROTOCOL section 4 preamble | `docs/decisions/delivery-guarantee.md` | in progress; one commit `ab3eab9` reportedly landed the claim TTL |
| `SoftTimeLimitExceeded` identity rebind, and the `float(cd)` guard in `_exec.py` | `docs/decisions/retry-name-matching.md` | in progress, may be uncommitted |
| 4 hour failure path soak on redis 6405 | `/tmp/cauli-soak-fail-14400.rolling_summary.latest.json` | killed by the shutdown. Last read at 215 samples: RSS flat at 50 MB, dead letter stream holding 1003 against 211k writes. Rerun from `/home/blackdevil/rupy-soak/`. |

## A mistake I made, recorded rather than hidden

Commit `8ce73f3` was meant to contain only `docs/AUDIT_LOG.md` and `docs/decisions/`. The packaging
agent had files staged in the shared index at that moment, so the commit also swept in
`.github/workflows/*`, both pyproject files, `worker/Cargo.toml`, `worker/Cargo.lock`,
`py/cauli/__init__.py` and `worker/wheel-data/scripts/cauli-worker`.

Nothing was lost and the content is intact, but that packaging work is now committed under a
documentation commit message rather than its own. It was not rebased out, because the machine was
shutting down and rewriting history under time pressure is how work actually gets destroyed. Someone
should split it, or simply note it in the changelog and move on.

This is the sixth git collision of the night on a single shared checkout. The rule that kept every
earlier one recoverable was staging by explicit path and checking `git diff --cached --name-only`
immediately before committing. I did not check it that time.

## Never verified, and it matters

**The full test suite has never run against the final tree.** The last combined verification covered
commit 19 of 56. Since then `arbitrary_precision`, the cpu batch, the counter gating, the contrib
rename and everything above have landed. `itest` in particular, the only Python to binary surface,
has not run against the `serde_json::Number` change.

Run before trusting anything: `cargo test --release --features test-hooks` in `worker/`,
`cargo fmt --check`, `cargo clippy --release --features test-hooks -- -D warnings`, pytest in `py/`,
pytest in `itest/`. Expected roughly 99 Rust, 253 py, 26 itest, but those numbers predate the
in flight work.

Everything runs in WSL `Ubuntu-24.04` with `CARGO_TARGET_DIR=/home/blackdevil/rupy-target` and
pytest at `/home/blackdevil/rupy-venv/bin/pytest`.

## What to do first when you come back

1. Run the full suite. Nothing else matters until the tree is known green.
2. Diff `audit/dangling-archive-f2e3151` against HEAD and decide whether it holds anything unique.
3. Deal with any uncommitted partial work from the in flight agents above, using the decision
   documents rather than trying to reconstruct intent from the diff.
4. Then the 1.0 blockers in `docs/decisions/release-readiness.md`.

---

## UPDATE, later the same session

Two of the four in flight items LANDED before shutdown. Corrected state:

| item | state now |
|------|-----------|
| Delivery guarantee | **DONE**: `ab3eab9` claim TTL derived from execution, `ee9af3b` claimant id in duplicate results, `a58b98c` retry delay clamped at 30 days, `27c4c98` PROTOCOL section 4. 113 Rust tests, clippy clean. |
| Packaging | **DONE**, but committed inside `8ce73f3`, see below. cp314 everywhere, the loader wrapper, an unmaskable CI leg, version 1.0.0 in all four gated places. |
| Observability | still in flight, partial edits may remain in `worker/src/{stats,exec,pyrt}.rs` |
| SoftTimeLimitExceeded rebind and the `float(cd)` guard | still in flight, partial edits may remain in `worker/src/shim.py`, `py/cauli/_exec.py`, `py/tests/test_exec.py` |

### The claim TTL fix, concretely

With `idemp_ttl` 60s and a 300s task, the guard key previously expired 240 seconds BEFORE the task
could finish, so the next redelivery claimed Fresh and ran concurrently. That was exactly the
duplicate execution the key exists to prevent. It now claims 302s, and every retry or redelivery
pushes it back to 302s, so a four attempt chain holds the key continuously.

### The packaging fix reproduced the real blocker

On a uv managed CPython 3.13 under `env -i`: the raw binary died with
`error while loading shared libraries: libpython3.13.so.1.0`, exit 127. Through the new console
script wrapper: `cauli-worker 1.0.0`, exit 0. The wrapper was tested with 17 assertions against two
interpreters, 34 of 34, including that it uses `execv` so the pid is unchanged, which matters because
this project relies on PDEATHSIG and a supervisor.

It also found and fixed a latent break that would have failed the release job outright:
`pip install --no-index --find-links dist cauli cauli-worker` cannot resolve cauli's own `redis` and
`msgspec` dependencies. Both workflows now name the two wheels by path.

### One regression the packaging fix introduces, needs a decision

The shipped binary's log target is now `cauli_worker_bin` rather than `cauli_worker`, because the raw
binary was renamed so that a plain `cargo build` keeps producing `cauli-worker` for itest, bench and
the docs. `RUST_LOG=debug` is unaffected, but **`RUST_LOG=cauli_worker=debug` now matches nothing in
a wheel build.** Nothing documents a per target directive today, so nobody is provably broken, but it
should be either aliased or documented before the tag.

### Correcting the record on 8ce73f3

The packaging agent reported that "another agent" swept its working tree into a documentation commit.
That was me, not another agent. I staged `docs/AUDIT_LOG.md` and `docs/decisions/` while that agent
had eight files sitting in the shared index, and I did not run `git diff --cached --name-only` before
committing, which is the exact check that had caught every earlier collision. The content is byte
identical to what was tested and the wrapper kept mode 100755, so nothing is lost, but the
attribution in that commit message is wrong and the packaging work deserves its own.

### Also fixed

`ruff format --check` was failing at HEAD on `py/cauli/beat.py` and `itest/test_integration.py`,
pre existing and unrelated to tonight's work. Fixed in `b3d5fad`; 48 files now pass.

---

## FINAL STATE. This section supersedes both sections above.

**82 commits on `audit/overnight`. `main` untouched at `6256854`. Nothing pushed to any remote, by
explicit instruction.** Working tree clean.

### Verified green as one combined state, at final HEAD

| check | result |
|-------|--------|
| `cargo test --release --features test-hooks` | 125 passed, 0 failed (115 unit, 10 across 7 e2e binaries) |
| `cargo clippy --release --features test-hooks -- -D warnings` | exit 0 |
| `cargo fmt --check` | clean |
| `pytest py/` | 255 passed |
| `pytest itest/` | 26 passed |
| ruff check and format | clean |

This closes blocker B1, which stood open for most of the session and which an earlier version of this
file overstated.

### Decision documents: 7 of 9 implemented

Implemented: observability, delivery guarantee, packaging, process model, retry name matching,
plus the release readiness blockers B1, B2 and B5.

**NOT implemented, and these are the two things to pick up first:**

| document | what it asks for |
|----------|------------------|
| `docs/decisions/error-taxonomy.md` | Add `error.origin` as an additive field, and rename the worker minted `TimeoutError` to `TimeLimitExceeded`. Until then `except TimeoutError:` around `.get()` silently misses a worker enforced timeout. Measured blast radius: 3 mint sites, 11 test assertions, about 4 PROTOCOL lines, zero Python matchers. |
| `docs/decisions/clock-architecture.md` | The `RedisClock` sampled offset, about 150 lines, plus `COUNT = min(batch, permits)` in the fetch loop. The second one closes a duplicate execution window that is real under saturation. |

`docs/decisions/redis-cluster.md` recommends refusing to start against a Cluster, which was never
implemented either; the docs now say plainly that Cluster is unsupported, which was the urgent half.

### Still open, and they need a human rather than another agent

- **B3**: 14 commits are flagged "needs human review before merge" in the audit log header. They
  change process termination, durability and public API.
- **B4**: the 4 hour failure path soak was stopped. Two shorter runs exist and are recorded. The open
  question is whether the failure machinery carries a small residue or simply warms up more slowly;
  the most suspicious explanation, the cpu recycle path, is already ruled out across 649 forks.
- The 15 second wedge detection window and the NOGROUP self heal choice are both judgement calls made
  by an agent and endorsed by me. Both are documented with their reasoning in the audit log.
- `mover_crossslot_is_detected_not_treated_as_transient` has a load sensitive timeout on a real
  cluster startup and failed intermittently under heavy parallel load. It will be flaky on a busy CI
  runner. Not fixed.

### Two archive branches, still do not delete without diffing

`audit/stash-archive-58cc0f8` and `audit/dangling-archive-f2e3151`. The second was recovered from a
genuinely dangling commit that would have been lost to garbage collection.

### The one regression this session introduced

`RUST_LOG=cauli_worker=debug` matches nothing in a wheel build, because the shipped binary's target is
now `cauli_worker_bin`. Documented in `docs/CONFIGURATION.md` rather than reverted, since the rename
is what keeps `cargo build` producing `cauli-worker` for the integration suite. `RUST_LOG=debug` is
unaffected.
