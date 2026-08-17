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
