# DECISION — Retry name matching, and what it uncovered
Produced on Fable. NOT implemented.

**Keep name plus countdown matching. It is the correct design, forced by two hard constraints, and
the collision is nearly behaviour neutral. The real pre 1.0 fix in this neighbourhood is a different
bug it uncovered: the `SoftTimeLimitExceeded` stand in identity break.**

## The Retry collision is much less serious than it looked

The feared outcome, silently swallowing a real error and rescheduling instead of failing, **cannot
happen, because cauli already retries every exception by default.** `shim.py:164` marks all non Retry
exceptions retryable, and `dispatch.rs:207-233` runs a forced retry and a retryable failure through
the SAME bounded path: the same `retries < max_retries` gate, the same `schedule_retry`, the same dead
letter with reason `max_retries`, and the same stored failure result carrying the user's own type,
message and traceback.

So for a user class named `Retry` carrying a `countdown`, the entire net delta is: the retry delay
becomes their countdown value instead of the computed backoff. Nothing else. The task still retries
`max_retries` times, still dead letters, and `.get()` still raises with their own type and full
traceback. Worst realistic case is a large countdown delaying the final failure.

And the case is largely self correcting: anyone writing a `Retry` class with a `countdown` attribute
is almost certainly expressing the Celery retry idiom, in which case cauli's interpretation MATCHES
their intent. Severity low, not a 1.0 blocker.

## Why identity matching is impossible, with evidence

Three verified constraints, any one of which would be sufficient:

1. **The shim can run before `cauli` is importable.** `pyrt.rs:208-215` executes shim.py's module body,
   including its `from cauli import ...` attempt at `shim.py:47`, BEFORE `load_app` inserts cwd, the
   extra paths and the `VIRTUAL_ENV` site packages. In the documented wheel in venv deployment the
   import succeeds; in the equally supported source built binary shape it fails, and there is then no
   canonical `cauli.Retry` object to compare against.
2. **Cauli-less apps are a promised contract.** PROTOCOL section 4.2 documents the duck rule
   explicitly, saying the worker's interpreter may not have cauli installed at all, and the entire
   worker e2e suite runs that way against a fixture defining its own `Retry`.
3. **The Rust cpu decision point only ever sees a string.** `ctx.rs:194` operates on JSON off the
   child's pipe, where class identity cannot cross the process boundary.

History confirms it was deliberate: `_exec.py` once used `isinstance` and audit M6 demoted it to name
matching so all three lanes share one rule, with a positive regression test pinning the duck
behaviour.

Both alternatives were rejected with reasons. isinstance first with a name fallback fixes nothing,
because the fallback still catches every collision. A marker dunder breaks version skew, since a 1.0
worker against an older installed cauli whose `Retry` lacks the marker would silently degrade, and it
still needs the name rule for cauli-less apps, so the collision survives anyway.

## THE REAL BUG: SoftTimeLimitExceeded is identity injected and the identity is wrong

This is the inverse problem and it is genuinely user visible. `shim.py:47`'s import runs before
`load_app`'s path setup, so in the source built or `VIRTUAL_ENV` deployment the shim binds its own
LOCAL STAND IN class, while the user's app imports the real `cauli.SoftTimeLimitExceeded`. The
watchdog then injects the stand in at `shim.py:320`, and the user's `except SoftTimeLimitExceeded:`
cleanup clause **does not match**.

Both `docs/CONFIGURATION.md:235` and `py/cauli/exceptions.py:29-34` advertise catch and cleanup as
supported. So this breaks an advertised behaviour, in a supported deployment shape, silently. Sync io
lane only: the cpu child imports real cauli, and the async lane never raises it at all per section 4.6.

Fix: rebind the shim's module global from `sys.modules.get("cauli")` inside `load_app` when it is
present. About 4 lines, shim.py only, no wire change. **Should land before 1.0.**

## Two smaller real defects in the same sweep

- **`_exec.py:245` calls `float(cd)` unguarded**, so a non numeric countdown replaces the user's
  actual error with a ValueError. `shim.py:158-162` already guards exactly this. About 3 lines, cpu
  lane only.
- **An undocumented lane divergence on `SerializationError`.** `ctx.rs:200` and `pyrt.rs:148` default
  `retryable` to `type != "SerializationError"`. The io lanes are immune because the shim always sets
  `retryable` explicitly, but the cpu lane falls back to that name default. So a user exception class
  named `SerializationError`, which is not far fetched in a Celery migration since kombu ships one,
  **retries on io and is terminal on cpu**. Cheapest fix is stamping `retryable: false` at the
  `_exec.py:296-301` mint site. Low urgency, loud and arguably correct semantics either way.

## Blast radius

The core decision is documentation only: one paragraph reserving `Retry` plus `countdown` as an
exception shape, which today is documented only in PROTOCOL and has zero user facing mention. Zero
code, zero wire, zero test churn, breaks nobody, since the current behaviour is already the tested and
specified contract.

The SoftTimeLimitExceeded rebind and the float guard are both small, non breaking, and repair
advertised behaviour rather than changing it.
