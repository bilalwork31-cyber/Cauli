# DECISION — 1.0 release readiness
Produced on Fable, having read the full audit log and spot checked every load bearing fix in source.
NOT implemented.

**Ship with conditions. Nothing known and unfixed still loses data silently on a supported topology.
But the final tree was never verified as one state, two promised tests do not exist, and the soak
verdict is outstanding. Merge after the checklist, not before.**

Its own summary of the situation, which is fair: **the code is better than the bookkeeping, and the
bookkeeping is where the blockers live.** Every fix it checked in source matches what the log claims.

## Blockers

| # | what | why it blocks |
|---|------|---------------|
| B1 | **No combined verification of the final tree.** The header's "combined state verified" line covers commit 19 of 43. `itest`, the only Python to binary surface, last ran BEFORE `arbitrary_precision`, the cpu batch and the counter gating landed. | A crate wide `Number` behaviour change shipped with the integration suite never run against it. |
| B2 | **The two cycle 29 tests never landed.** Grep confirms no crash redelivery idempotency test and no redelivery limit e2e anywhere. The log says "being written now". | The second path was CHANGED tonight and has zero coverage; the first is the highest stakes property a queue has. The audit rated both worth fixing before 1.0 and then did not. |
| B3 | The 13 flagged commits still need the human review the audit refused to self grant. | They change process termination, durability and public API. Source checks are corroboration, not review. |
| B4 | Failure soak verdict outstanding. | Free to collect. Tagging before it wastes the run. |
| B5 | Repo hygiene: `stash@{0}` holds five worker source files plus PROTOCOL.md of possibly unique work, the backup branch still exists, versions are still 0.1.0 Alpha, and `release.yml` has never been exercised. | Publishing a repo with a live stash of unmerged worker code is how a 1.0 ships a mystery. |

## Corrections it made to the audit's own record

- **Redis Cluster scoping was wrong, and I approved the wrong wording.** The README now says "not
  supported for delayed or periodic tasks", which implies plain tasks work. The worker links no
  cluster protocol at all: `worker/Cargo.toml` enables only `tokio-comp` and `connection-manager`,
  independently verified. So a real multi node cluster MOVED fails ORDINARY operations too. The repro
  used a single node cluster, which hides exactly that. The line must read "Redis Cluster is not
  supported", unqualified.
- **A stale row in the decisions table**: the CROSSSLOT duplicate publish under a retrying client is
  moot, because that fallback path was deleted and cluster now raises before either failure can occur.
  Struck.
- **The dead `KEYS[2]` item is now attested**, not merely inspected: KEYS[2] genuinely carries the rev
  HGET and HSET.
- **Sloppy bookkeeping caught**: the async submit thread panic isolation was filed under "closed or
  explained" and never actually explained. The outcome is still fine, since only cauli's own Rust runs
  on that thread and every async task exercises it, but the filing was wrong.

## The four categories the audit missed entirely

1. **Broker state loss.** Redis was frozen and killed, never restarted EMPTY. `fetch_loop` treats
   NOGROUP as a generic warning with a 500ms retry forever, and `ensure_groups` runs only at startup.
   So a redis restart without persistence, which is the ElastiCache default and also what an OOM kill
   or a DR restore looks like, leaves every worker alive, deaf and quietly warning until a human
   restarts it, with the delayed sorted set simply gone. There is zero persistence guidance anywhere
   in the docs. The at least once guarantee silently assumes the pending entries list is immortal.
2. **Upgrade and deploy choreography.** No mixed version test exists, and tonight's own result key fix
   created a new failure: during a rolling deploy an old worker receiving a NEW task name now
   terminally dead letters it AND writes a final result, so the client stops waiting. The task is lost
   rather than picked up by a new worker. Deploy order, workers before producers, is load bearing and
   stated nowhere.
3. **The producer side under async.** The flagship story is FastAPI, but `.delay()` is synchronous
   redis I/O: inside an async handler it blocks the event loop, for up to the new 5 second socket
   timeout per call when redis degrades. There is no async enqueue API and no warning. The audit
   audited the worker half of the FastAPI story and never the enqueue half.
4. **The release pipeline and support matrix**, covered in the packaging decision document.

Smaller: no written threat model, and redis `maxmemory` exhaustion on the write path is only
indirectly covered.

## The one next thing

**A one day broker loss cycle.** Restart redis empty and stale under a live worker and beat; make
NOGROUP either self heal by rerunning `ensure_groups` or fail once loudly; write the persistence
requirements section; and land the two missing redelivery tests in the same harness, since they share
the kill and restart machinery. It is the only remaining class where a routine operational event makes
a documented guarantee false with no loud signal, and it closes B2 in the same stroke.

## Its disagreements with my calls, which I accept

- `cauli.contrib.fastapi` should be renamed to `cauli.contrib.sqlalchemy` with `fastapi` kept as a
  reexport alias. It imports nothing from FastAPI, Litestar users will never find it, and a real
  FastAPI integration will want that namespace later. Under an hour, and pre 1.0 is the only cheap
  time.
- The 48 hour soak bar should NOT hold 1.0. The historical reason for that bar, unexplained slow
  growth, now has a mechanism, a fix and two flat soaks. Run 48 hours as post release routine.
- The thread state pinning is now a correct design rather than a tolerated leak, because the ceiling
  turned its one real hazard into a bounded observable one.
