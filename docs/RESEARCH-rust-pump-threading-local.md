# Research: a Rust pumped loop vs Django's `threading.local` semantics

Date: 2026-08-10. Method: kira council (L, Light, Near) + TITAN.
Oracles: the installed sources in WSL `~/b5-venv` (Django 6.1, asgiref 3.12.1,
psycopg2 2.9.12, Python 3.12.3), this repo, and five experiments run against the
bench Postgres (127.0.0.1:54402, isolated by `application_name`). Experiment
scripts live in WSL at `~/pump-lab/e1..e5*.py`; every output block below is
pasted from a run watched happen. No Rust was built, no load test was run.

## Verdict, first

**The pump is not worth building now, and the design that would make it safe is
worth building anyway.** The problem statement assumes Django caches connections
on the coroutine's thread. The installed sources say it does not: on the legal
path Django refuses ORM calls on any thread with a running loop
(`SynchronousOnlyOperation`, measured) and routes all sync DB work to executor
threads via `sync_to_async(thread_sensitive=True)`. The thread bound state
therefore already lives on threads a pump never touches. The correct design is
to promote that accident into an invariant: **evacuate all thread bound state to
M long lived sticky executor threads, assigned per task through the contextvar
asgiref itself reads.** Prototyped and measured here on real Django + real
Postgres: one connection per task across awaits, reuse across tasks, exactly M
backends, and a measured 348 ms to 48 ms fix of a serialization wall cauli's
async ORM path has **today**. With state evacuated, a future pump may hop
freely; until the broker path scales past ~25k tasks/s per box, its payoff
cannot exceed the 60 to 100 µs of loop machinery that a ~5 line uvloop swap
already attacks.

---

## 1. Verified vs assumed ledger

Every load bearing claim, with its oracle. [V] = verified against the installed
source or a watched experiment. [A] = assumed, and labelled with why.

| # | Claim | Status | Oracle |
|---|---|---|---|
| V1 | Django stores DB connections in `Local(thread_critical=True)` | V | `django/db/utils.py:141-148`: "Connections needs to still be an actual thread local, as it's truly thread-critical. ... There's no cleanup after async contexts, though, so we don't allow that if we can help it." `thread_critical = True`. Storage wired at `django/utils/connection.py:41`: `self._connections = Local(self.thread_critical)`; cache miss opens a new connection at `connection.py:56-64`. |
| V2 | `thread_critical=True` means: sync thread → plain `threading.local`; async thread → a `_CVar` (contextvar storage) held INSIDE `threading.local` | V | `asgiref/local.py:111-116` (`self._storage = threading.local()`), `local.py:130-135` (sync branch), `local.py:142-149`: "Ensure context exists in the current thread / if not hasattr(self._storage, 'cvar'): self._storage.cvar = _CVar()". A hop to a new thread finds no `cvar` and mints a fresh one, so the old value is unreachable even though the Context traveled. |
| V3 | Non critical `Local` (contextvars backed) is THREAD TAGGED: values carried into a different thread are silently discarded | V | `asgiref/local.py:49-52`: "Only return storage that belongs to the current thread. Storage with a different thread id was inherited by this thread ... and must not be visible." Rehoming happens only at asgiref's own boundaries: `asgiref/sync.py:37-52` (`_restore_context`) and `sync.py:503-508`. A Rust pump is not such a boundary. |
| V4 | Django loudly refuses ORM calls on any thread with a running loop unless `DJANGO_ALLOW_ASYNC_UNSAFE` is set | V | `django/utils/asyncio.py:16-24` (`get_running_loop()` probe, `SynchronousOnlyOperation`). Measured in E1 variant A0 below: the pump facade thread got the exception and zero connections were opened. |
| V5 | Django 6.1's async ORM is sync ORM on an executor thread; nothing native | V | `django/db/models/query.py:694-695`: `async def aget(...): return await sync_to_async(self.get)(*args, **kwargs)`. Grep for `supports_async` / `acursor` in `db/backends`: no hits. Connection pooling requires psycopg3 (`django/db/backends/postgresql/base.py:191-212`, imports `psycopg_pool`); the installed driver is psycopg2. |
| V6 | With no context set, ALL `thread_sensitive=True` work in the process funnels into ONE global thread | V | `asgiref/sync.py:409` (`single_thread_executor = ThreadPoolExecutor(max_workers=1)`), selection ladder `sync.py:462-489`, fallback at `486-489`. Per scope executors exist via `ThreadSensitiveContext` (`sync.py:110-128`, `467-478`), keyed by instance in `context_to_thread_executor` (`sync.py:425-427`). Django wraps each HTTP request in one: `django/core/handlers/asgi.py:172`. cauli wraps tasks in nothing, so its async ORM tasks share the single global thread today. |
| V7 | The global funnel is a real wall, and M sticky executors remove it | V | E4 measured: 64 concurrent 5 ms sleeps through the global executor take 348 ms wall; the same load with M=8 sticky executors takes 48 ms. No op round trip 168 µs (see A4 for the caveat). |
| V8 | psycopg2 connections may legally cross OS threads; sequential handoff and even a transaction spanning two threads work | V | `psycopg2.threadsafety == 2` (printed by E2; PEP 249 level 2 = "threads may share the module and connections"). E2 measured: same connection object queried from two threads, same server backend, then a transaction opened on thread A and committed on thread B returned sum=3. The illegal thing is concurrent use of one connection, not handoff. What breaks on a hop is CACHE VISIBILITY, not the connection object. |
| V9 | For SQLite, Django disables the driver's own cross thread guard and relies entirely on the `Local` isolation | V | `django/db/backends/sqlite3/base.py:180`: `kwargs.update({"check_same_thread": False, "uri": True})`. Under a hopping pump plus the override env var, that isolation is exactly what shears. |
| V10 | The hop failure itself: two Django DB calls across a hopped await duplicate connections and orphan the first | V | E1, output pasted in section 2. Variant B (bare threads): backends 1 → 2, different connection object and backend per side, `close_old_connections` on the resumed thread closes only its own, the orphan lives until its OS thread exits. Variant A (loop facade + override): same duplication; orphan owned by the task Context instead, freed only when the context dies. |
| V11 | Direct ORM from coroutines (override users) is connection churn per task ALREADY, on today's stable loops, no pump involved | V | E5 measured: within one task, reuse across an await works on a stable thread; the NEXT task got a different connection despite `CONN_MAX_AGE=600`; backends tracked the set of live task objects. A pump cannot be blamed for a path that never reuses across tasks in the first place. |
| V12 | cauli today: N asyncio loops on dedicated threads; sync pool pins persistent thread states; the async submitter thread deliberately does not pin | V | `worker/src/shim.py:377-394` (`start_loops`, one daemon thread per loop); `worker/src/pyrt.rs:392-414` (the pinned thread state comment: an unpinned `Python::attach` "wipes `threading.local` storage between tasks ... Django caches its DB connection per thread"); commit `8ddf603`: "The pinning requirement in this file applies to threads that execute task bodies ... this thread never runs user code." |
| V13 | The 21% CPU executor hop finding was about the HOOK running on every task, and the latch already gates it | V | `py/cauli/contrib/django.py:127-133`: "the two thread hand-offs per task are pure overhead — measured at 21% of worker CPU on an async HTTP workload, worth +61% throughput when skipped. Once a connection exists the flag latches on". The latch is process wide (`django.py:87-98`) and keeps working unchanged under every design below. |
| V14 | bench3's async DB numbers exercise asyncpg pools, not the Django ORM | V | `bench3/workloads_async.py:14, 72-86`. The 0.22 ms per task figure contains no ORM executor hop. |
| V15 | Sticky executor injection through asgiref's public attributes works on the installed versions | V | E4 part 4: 16 tasks over 8 pre seeded executors, `thread_sensitive_context.set(key)` inside each task's own context; same connection within a task across an await, same connection across tasks sharing an executor, exactly 8 server backends. |
| A1 | The pump's contract is as django-bolt documents: "callbacks may resume on different OS threads post-suspension, with contextvars preserved but threading.local state not carried across awaits" | A | Taken from the task brief; django-bolt's source was not read here. All experiments implement exactly this contract (same `contextvars.Context`, different OS thread). |
| A2 | CPython offers no hook to intercept attribute writes on `_thread._local` instances, so runtime detection of arbitrary `threading.local` use is not viable | A | No such hook exists in the documented C API, `sys.monitoring`, or audit events as of 3.12. Basis for moving detection to CI (section 4). A cheap counterexample would change the enforcement story, not the design. |
| A3 | Upstream Django may eventually ship a native async ORM; no timeline | A | Installed 6.1 shows none (V5). Treat as "recheck per release", not as a plan. |
| A4 | E4's 168 µs round trip is an upper bound flavored number | A | Measured under `nice -n 19` while the benchmark campaign occupied the pinned cores. The production grade number for the same hop is the project's own 21% CPU finding (V13). Order of magnitude agrees; do not quote 168 µs as clean. |
| A5 | E5 residual: after dropping the second task inside the running loop, its backend persisted through gc within the 2 s poll | A | Closed at loop teardown. Cause not chased (suspect: a lingering reference from the loop's bookkeeping). Does not touch the churn finding, which is about the SECOND task opening a new connection. |
| A6 | The regime numbers: 0.22 ms per task CPU, 60 to 100 µs loop machinery, ~78k ops/s Redis ceiling, 3.1 Redis ops per task, ~25k tasks/s broker wall | A | Established by bench3 on this box per the task brief; not remeasured here (the campaign owns the cores). |

---

## 2. The captured failure case

`~/pump-lab/e1_pump_hop.py`. One coroutine, two Django DB calls separated by an
await. The driver executes segment 1 on long lived OS thread T1 and segment 2 on
T2, carrying the SAME `contextvars.Context` across the hop: exactly the pump
contract (A1). Variant B models bare pump threads (no loop facade → asgiref's
sync branch, plain `threading.local`). Variant A models a facade that registers
itself as the running loop (`asyncio.events._set_running_loop`) with the
override env var set (→ the `_CVar` branch). Variant A0 is the facade without
the override. Postgres backend counts are filtered by `application_name`, so the
running campaign never enters the numbers.

Exact output:

```
django 6.1, pid 450007
=== Variant B: bare pump threads (no loop facade; plain threading.local) ===
[B] backends before: 0
[B] step1: thread=133316531451584 conn_id=133316564473536 backend_pid=450010 handler_sees=1
[B] step2: thread=133316523058880 conn_id=133316564473856 backend_pid=450012 handler_sees=1
[B] same OS thread: False   same connection object: False   same server backend: False
[B] backends after hop task: 2  (expected 2: one per thread)
[B] resumed thread's handler sees 1 conn(s); close_old_connections there
[B] backends now: 1  (T1's connection is ORPHANED: no hook can reach it)
[B] backends after pump threads exit + gc: 0

=== Variant A0: loop facade, DJANGO_ALLOW_ASYNC_UNSAFE unset ===
[A0] loud refusal: SynchronousOnlyOperation: You cannot call this from an async context - use a thread or sync_to_async.
[A0] backends: 0  (no connection was ever opened)

=== Variant A: loop facade + DJANGO_ALLOW_ASYNC_UNSAFE=1 (asgiref _CVar branch) ===
[A] step1: thread=133316531451584 conn_id=133316564473536 backend_pid=450015 handler_sees=1
[A] step2: thread=133316523058880 conn_id=133316564473856 backend_pid=450017 handler_sees=1
[A] same OS thread: False   same connection object: False   same server backend: False
[A] backends after hop task: 2
[A] resumed thread's handler (in task context) sees 0 conn(s); close_old_connections there
[A] backends now: 1
[A] backends after T1 exits (context still alive): 1
[A] backends after task context is dropped + gc: 0
```

The mechanism, named:

- **Variant B** (sync branch): the handler cache is plain `threading.local`
  (`asgiref/local.py:130-135`). Step 2 on T2 finds nothing, opens a second
  connection. `close_old_connections` iterates
  `connections.all(initialized_only=True)` (`django/db/__init__.py:57-59`),
  which can only see the CURRENT thread's storage, so the after hook run on the
  resumed thread closes only T2's connection. T1's connection is orphaned; it
  survives exactly as long as the pump thread does, which in production is
  forever. `CONN_MAX_AGE` is meaningless for it. This is the same wound
  `worker/src/pyrt.rs:392-414` documents from the thread state catastrophe,
  reopened one layer up.
- **Variant A** (facade branch): storage is a `_CVar` inside `threading.local`
  (`asgiref/local.py:142-149`). T2 has no `cvar` attribute, mints a fresh one,
  and the step 1 connection becomes unreachable even though its bytes sit in
  the still live task Context. Note the sharpened line: OUTSIDE the task
  context the handler sees zero connections even on the right thread; inside
  the task context on the resumed thread it sees only the post hop connection.
  The orphan's owner is the task Context, so it dies with the task instead of
  with the thread: churn per hop rather than a permanent leak, and equally
  fatal to `CONN_MAX_AGE` reuse.
- **Variant A0** is the load bearing good news: with the override unset, the
  legal path cannot even reach this failure. Django's own tripwire
  (`django/utils/asyncio.py:16-24`) fires before a connection exists.

Two companion captures bound the blast radius:

**E3, the silent class** (`e3_thread_tagged_local.py`). Django state held in
NON critical `Local`s (`django/utils/timezone.py:61`,
`django/utils/translation/trans_real.py:26`, `django/urls/base.py:16,19`) is
contextvars backed but thread tagged (V3):

```
activate('Asia/Karachi') then read, same thread, same context : Asia/Karachi
read again, SAME thread, same context                          : Asia/Karachi
read after hop to OTHER thread, SAME context                   : UTC
silently reverted to settings.TIME_ZONE: True; no exception was raised
```

No exception, wrong timezone. Under a hopping pump this class does not fail, it
lies. Any acceptable design must convert this to a loud failure (section 4).

**E5, the control** (`e5_today_override_churn.py`). Today's architecture, one
stable loop thread, override env var set, no pump anywhere:

```
task1 reused its connection across the await (stable thread): True
task2 got the SAME connection as task1: False  <- CONN_MAX_AGE=600 asked for reuse
backends while task1 alive: 1; while both task objects alive: 2
```

Direct from coroutine ORM already opens one connection per task and never
reuses across tasks, because `thread_critical` storage in an async thread is
per Context (V2) and "there's no cleanup after async contexts" (V1). The pump
makes this path worse (duplication per hop), but it did not break it; it was
born broken. The only path that works today is the executor path, and that one
is pump proof by construction, which is the entire design insight.

---

## 3. The candidates, TITANed

### A. Task to thread affinity

- **Mechanism.** Pump keeps N threads; every continuation of a task is requeued
  to that task's home thread; Rust owns timers and wakeups.
- **Guarantees.** `threading.local` semantics identical to today, by
  construction (I1): the failure in section 2 is unreachable.
- **Costs, both directions.** Per home thread head of line blocking, exactly as
  today's N loops. Worst case unchanged from the current architecture, which is
  the point and the problem: it IS the current architecture. The pump still
  needs a full asyncio loop facade for compatibility.
- **Deletes.** Nothing on the Python side. The I2 harvest list is empty, and a
  direction that deletes nothing is not a solution.
- **Failure mode + catcher.** None new; that is its only virtue.
- **Verdict: rejected as a goal.** This is today's N loops with Rust timers
  bolted on; uvloop buys the same class of win for ~5 lines. It survives only
  as the default eligibility class in section 4 (a task is pinned unless it
  declares otherwise).

### B. Capability scoped pinning

- **Mechanism.** Per task `hop_safe` flag in the registry (fits the existing
  `kind` routing, `worker/src/pyrt.rs:34-39`); pinned by default; hop eligible
  tasks may be work stolen.
- **Named failure mode.** *Silent state shear*: a task declares `hop_safe`,
  then touches `threading.local` or a thread tagged `Local`; behavior is E3,
  wrong answers with no exception.
- **The catcher, honestly costed.** Runtime interception of arbitrary
  `threading.local` writes is not viable (A2). So the mechanism is layered:
  1. **Fail safe default**: hopping is opt in, never inferred.
  2. **A deterministic CI hop runner**: the E1 driver generalized into a test
     utility that executes a task forcing EVERY await boundary onto a different
     OS thread while preserving the Context. Pure Python, exists today (it ran
     section 2). A lying task diverges or trips its own assertions in CI, not
     in production. This is A4 discipline: the test requires the property, it
     does not merely permit it.
  3. **Prod sentinel for the Django case**: after a hopped task's body returns,
     check `connections.all(initialized_only=True)` from inside the task
     context; non empty means someone did direct ORM on a pump thread; fail the
     task with an error naming it (E1 variant A proves the handler sees exactly
     those connections there).
- **Deletes.** For hop eligible tasks: per loop head of line blocking.
- **Verdict: adopted**, as the eligibility layer of the recommendation, not as
  a standalone answer, because it does not by itself make the flagship ORM
  case hoppable. Section 4's evacuation does that.

### C. Executor delegation

- **Mechanism.** Pump threads never run thread bound code; ORM and sync work go
  through `sync_to_async(thread_sensitive=True)`.
- **The oracle's correction.** This is not a proposal, it is how Django already
  works: `aget` IS `sync_to_async(self.get)` (V5), and the worker's own hooks
  already route through it (`py/cauli/contrib/django.py:158-162`). The pump
  changes nothing for this path: the executor thread holds the state and the
  executor thread does not hop. The routing state (`thread_sensitive_context`,
  `deadlock_context`) is plain contextvars, which the pump preserves.
- **What made it unaffordable before, and why not here.** The 21% CPU finding
  (V13) was the HOOK paying two hops on every task including tasks that never
  touch the DB; the `connection_created` latch fixed that and is design
  independent: tasks that never open a connection never pay, under every
  candidate here.
- **The hidden wall this research found.** Without a context, every
  `thread_sensitive` call in the process lands on ONE global executor thread
  (V6). Measured (E4): 64 concurrent 5 ms sleeps → 348 ms wall, fully
  serialized. cauli's async ORM tasks sit behind this wall TODAY, pump or no
  pump. Django's per request answer (`ThreadSensitiveContext`, one per HTTP
  request, `asgi.py:172`) buys parallelism by spawning an executor per scope,
  which is per task thread churn if copied naively.
- **Verdict: correct direction, incomplete.** It needs the executor wall fixed
  and an enforcement story for the residue. Both are section 4.

### D. Context backed locals

- **Mechanism.** Alias `threading.local` / thread critical storage to pure
  contextvars so state follows the task.
- **Why Django's flag exists, from the source.** `django/db/utils.py:143-147`
  is explicit, and the deeper mechanism kills the idea independently:
  contextvar inheritance is COPY, not exclusive transfer. `create_task` copies
  the parent Context, so a connection that "follows the context" fans out to
  every sibling task spawned under it; concurrent coroutines then share one
  connection and one transaction. psycopg2's level 2 makes cross thread
  handoff legal (V8); it does not make two tasks interleaving statements
  inside one transaction sane. And there is no cleanup point: "There's no
  cleanup after async contexts" (V1), which E5 measured as churn. asgiref's
  thread tag (V3) exists precisely to stop context inherited storage from
  leaking into unrelated threads; this direction would delete the safety and
  keep the leak.
- **Verdict: dead. Cause named: copied contexts fan ownership out; a
  connection needs exactly one owner and a close point, and contexts provide
  neither.** Published per N3, do not reopen.

### E. Django version delta

- Installed reality (V5): Django 6.1 `aget` wraps sync; no async cursors; no
  native async backend; pooling requires psycopg3. The constraint is not
  dissolving in the version cauli targets.
- When upstream ships a native async ORM (A3, no timeline), connections become
  per context pool leases and this entire problem class evaporates upstream
  (T4). Until then the executor path is Django's answer, and section 4 builds
  on it rather than around it.
- **Verdict: monitor per release; not a plan.**

---

## 4. The recommended design: evacuate the state, do not pin the coroutine

**Invariant (I2, promoted from "usually true" to "always true"): no task
visible thread bound state ever lives on a loop or pump thread. All thread
bound work runs on M long lived sticky executor threads, and a task's
assignment to its executor travels in contextvars, which every pump preserves
by contract (A1).**

This is candidate A's affinity applied to the STATE instead of the coroutine,
through candidate C's channel, guarded by candidate B's eligibility layer. It
reparameterizes until the fast path is legal (I1): the property that
disqualified hopping was "state lives on the stepping thread", so the design
makes "state never lives on the stepping thread" true by construction, and the
pump stops being dangerous instead of being defended against. TITAN I5 also
applies: scheduling granularity (any thread) and state granularity (sticky
threads) are decoupled, and the constraint flows one way only: DB work goes to
sticky threads; coroutine steps go anywhere.

### Mechanism (prototyped in E4, output below)

asgiref already has the routing hook; cauli only has to seed it:

```python
# worker startup (M configurable, e.g. --db-executors, default small):
class _StickyKey:  # stands in for a ThreadSensitiveContext instance
    pass

_KEYS = [_StickyKey() for _ in range(M)]
for key in _KEYS:
    SyncToAsync.context_to_thread_executor[key] = ThreadPoolExecutor(max_workers=1)

# shim._arun, before hooks and user code (inside the task's own Context,
# so the assignment can never leak between tasks):
SyncToAsync.thread_sensitive_context.set(_KEYS[token % M])
```

Every `thread_sensitive=True` call inside that task, which includes `aget`,
`sync_to_async` bodies, and cauli's own
`sync_to_async(close_old_connections, thread_sensitive=True)` hook
(`py/cauli/contrib/django.py:158-162`), then lands on that task's sticky
executor (`asgiref/sync.py:467-478` takes the context branch before the global
fallback). Connections cache in the executor thread's plain `threading.local`,
the oldest and best tested branch of V2.

E4's measured run of exactly this mechanism, against the bench Postgres:

```
1. sync_to_async(thread_sensitive=True) no-op round trip: 168.2 us
2. 64 concurrent 5 ms sleeps, NO context -> global single executor: 348 ms wall
3. same load with M=8 sticky executors via thread_sensitive_context: 48 ms wall
4. 16 tasks x 2 DB calls across an await: same conn within task: True;
   tasks on same sticky executor reuse the SAME conn: True;
   distinct server backends: 8 (== M=8); pg_stat_activity: 8
```

### What the guarantee costs (I3, both directions)

- **Per ORM call:** one executor hop. Upper bound flavored 168 µs here (A4);
  the honest production number is the project's 21% CPU finding for two hops
  per task (V13). This is not a new price: it is the price of Django's legal
  path, paid today by `aget` and by Django's own async views. The latch keeps
  it at zero for tasks that never open a connection.
- **Resources:** M threads and at most M connections per database alias.
  Existence: every task has a home by construction (assignment is a total
  function of the token). Tightness: only M threads can ever call `connect` on
  the legal path, so the backend count cannot exceed M × aliases; E4 measured
  exactly 8 at M=8. Override users can exceed it, and the sentinel below
  converts them from a leak into a named error.
- **Worst case, one direction:** M too small serializes DB heavy tasks behind
  M threads (head of line at executor granularity). Expose per executor queue
  depth in worker stats so the failure names its constraint (Near). Worst
  case, other direction: M too large approaches Postgres `max_connections`;
  the bound is explicit and configurable, which is what a pool is.
- **Laziness of `CONN_MAX_AGE`:** an idle executor's stale connection closes
  on the next task routed there, same lazy semantics as Celery's per task
  `close_old_connections`. No new mechanism needed.

### The harvest (I2): what the invariant deletes

1. **The global executor wall**: 348 ms → 48 ms measured at M=8. This lands
   TODAY, on the current architecture, before any pump exists.
2. **Per scope executor spawn**: Django's per request `ThreadSensitiveContext`
   pattern is never adopted, so its thread churn is deleted before being added.
3. **Coroutine pinning as a Django safety requirement**: with state evacuated,
   the scheduler is free. Today that means the N loops can become uvloop loops
   or change shape without touching Django semantics; under a future pump it
   means work stealing is legal for every task that passes the eligibility
   gate.
4. **For the future pump:** the per thread pinned state ceremony
   (`worker/src/pyrt.rs:392-414`) stays only where task bodies genuinely live
   (the sync pool and the sticky executors, where CPython manages it
   automatically since they are Python born threads); pump threads need it
   only as a perf nicety, not for correctness.
5. **`install_db_hooks` stays byte identical**: latch, hop, and close logic
   already route through the exact channel the design standardizes (V13).

### Adversary (A1 to A4)

Degenerate solution, named: *make the benchmark fast by hopping everything and
letting Django break quietly*. Blocked structurally: hopping is opt in per
task (B), the acceptance tests below make Django state load bearing so the
degenerate path scores badly, and the silent classes are converted to loud
ones:

| Failure mode | Mechanism that catches it |
|---|---|
| Direct ORM on a loop/pump thread via `DJANGO_ALLOW_ASYNC_UNSAFE` (E1 variant A; E5 shows it is already churn today) | Worker warns at startup if the env var is set; after each task body, `connections.all(initialized_only=True)` checked from inside the task context; non empty fails the task with its name (E1 A proved the handler sees exactly those connections there). Without the env var, Django's own `SynchronousOnlyOperation` already fires (E1 A0). |
| A task lies about hop safety and touches `threading.local` or a thread tagged `Local` (E3's silent class) | The CI hop runner: every await boundary forced onto a different OS thread, Context preserved; the lie diverges in CI. Runtime interception is not viable (A2), so CI is the gate for the `hop_safe` flag. |
| User code clears or overwrites the sticky assignment contextvar | Strict mode assertion in the after hook: `thread_sensitive_context` must hold a cauli key during task teardown. |
| M too small, DB tasks convoy | Per executor queue depth in stats; the diagnostic names the binding constraint instead of presenting as generic slowness. |
| SQLite user under a hopped task with the override set (V9: Django disabled the driver's guard) | Same sentinel as row 1; additionally document that the override env var is unsupported in pump mode, full stop. |

Wrong looks wrong (A3): every silent failure in this table is converted into a
named, per task error or a CI divergence. The one that cannot be (E3's
timezone/translation/urlconf activation inside a hop eligible task) is exactly
why hopping stays opt in.

### Acceptance tests, written before any implementation (Near)

Falsifiable, numbered, each fails if the feature is deleted (A4). Prototypes
for 1, 2, 5, 6 already ran as E4/E1/E3.

1. An async task performing two ORM reads across an await uses ONE connection:
   same `pg_backend_pid` for both reads, observed via `pg_stat_activity`
   filtered by `application_name`. (E4.4 shape: intra task True.)
2. Two different tasks assigned the same sticky executor reuse the SAME
   connection; total backends == M exactly, never more. (E4.4: inter task
   True, 8 == M.)
3. With `CONN_MAX_AGE=0`, after the after hook runs, this app's backend count
   returns to 0: the hook reached the thread that holds the connections.
4. With `CONN_MAX_AGE=1`, a task run 1.2 s after its predecessor on the same
   executor gets a NEW `pg_backend_pid` (expiry closed the old one) and the
   count still never exceeds M.
5. Loud, not silent: under the CI hop runner, a task that writes
   `threading.local` off its home thread fails its assertion in CI; a task
   doing direct ORM with the override set fails at runtime with an error that
   names the task, not with silent duplication. (E1/E3 shapes.)
6. The wall is gone: 64 concurrent tasks each doing one 5 ms
   `thread_sensitive` call complete in ≤ 2 × (64/M) × 5 ms wall. (E4: 48 ms at
   M=8 against 348 ms global.)
7. No regression for DB free tasks: with the latch never tripped, per task CPU
   on the no DB async workload is unchanged (the hop count stays zero;
   `py/cauli/contrib/django.py:127-133` semantics preserved).

---

## 5. Payoff arithmetic, and the pump verdict

The number in its regime (T1, from A6):

- cauli async per task CPU: **~0.22 ms**, of which asyncio loop machinery is
  **60 to 100 µs** (27 to 45%).
- The broker path spends **~3.1 Redis ops per task** against a measured
  **~78k ops/s** pinned Redis ceiling: the wall is **~25k tasks/s** on this
  box. Past it, no loop improvement buys throughput; it buys only CPU headroom
  and latency.
- The pump's theoretical best case is therefore bounded by those 60 to 100 µs
  per task, and a **~5 line uvloop swap** in `shim.start_loops` attacks the
  same budget with zero threading semantics change (uvloop keeps one loop per
  thread; every guarantee in this document holds untouched).
- Against that bounded win, the pump costs: a full asyncio loop facade, the
  eligibility machinery, and permanent exposure to the residue classes of
  section 2, of which one (E3) is silent by nature.

**Verdict: do not build the pump until the broker path scales past ~25k
tasks/s per box.** That sentence is the honest ceiling (N4). What IS worth
building now:

1. **The uvloop lever** (~5 lines, zero risk to the threading contract).
2. **Sticky DB executors** (this document's design, roughly a page of Python
   in the shim plus a worker flag): it fixes a measured 7x serialization wall
   in the flagship ORM integration today, bounds worker connection count at M
   like a pool, and is precisely the invariant that makes any future pump
   legal by construction rather than by discipline.

Negative results, published so nobody walks these again (N3):

- **D is dead**: copied contexts fan connection ownership out to sibling
  tasks and provide no close point. Cause named in section 3D.
- **A is empty as a goal**: task to thread affinity on a pump deletes nothing
  relative to today's loops; it is reinvention with Rust timers.
- **Runtime detection of `threading.local` writes is not viable** (A2);
  enforcement belongs in a deterministic CI hop runner, which this research
  already built in miniature (`~/pump-lab/e1_pump_hop.py`'s driver).
- **The pump does not break direct from coroutine ORM; it was never whole**:
  E5 measured connection churn per task on today's stable loops with
  `CONN_MAX_AGE=600` asking for reuse. The only ORM path worth protecting is
  the executor path, and evacuation makes it pump proof.

## Appendix: experiment inventory

All in WSL `~/pump-lab/`, run with `nice -n 19` under `~/b5-venv` against the
bench Postgres, isolated by `application_name`; total CPU spent is a few
seconds; nothing in bench2/bench3 state was touched; no scratch tables were
needed.

| Script | Question | Answer |
|---|---|---|
| `e1_pump_hop.py` | What exactly happens to Django connections when a coroutine hops threads across an await | Section 2: duplication, orphan, hook blindness; loud refusal on the legal path |
| `e2_psycopg2_threads.py` | Is sequential cross thread use of one psycopg2 connection illegal | No: threadsafety 2, handoff and cross thread transaction measured working |
| `e3_thread_tagged_local.py` | What happens to `timezone.activate` style state on a hop | Silent revert to default, no exception |
| `e4_executor_economics.py` | Cost of the executor path, the global funnel wall, and the sticky executor prototype | 168 µs niced; 348 ms → 48 ms at M=8; perfect connection stickiness at M backends |
| `e5_today_override_churn.py` | Does direct from coroutine ORM work on today's stable loops | Reuse within a task only; new connection per task despite `CONN_MAX_AGE=600` |
