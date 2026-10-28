# Retry is recognised by name, not identity

Status: Accepted (0.1.0)

## Context

A task forces a retry by raising `cauli.Retry(countdown=...)`. All three lanes
recognise it by class name plus a `countdown` attribute: `_is_retry` in
`worker/src/shim.py` for the io lanes, `_is_retry` in `py/cauli/_exec.py` for
the cpu child, and `parse_pyresp` in `worker/src/ctx.rs` on the Rust side of
the cpu pipe. Any user class named `Retry` with a `countdown` therefore counts
as a forced retry, and the question was whether that collision was acceptable.

Identity matching is not possible, for three independent reasons:

1. The shim's module body runs before `load_app` adds the working directory,
   extra paths and the `VIRTUAL_ENV` site packages to `sys.path`. In a source
   built worker with no venv link, `from cauli import ...` fails at that point
   and there is no `cauli.Retry` object to compare against.
2. Apps that do not install `cauli` in the worker's interpreter are supported.
   PROTOCOL.md section 4.2 documents the duck typed rule, and the worker's end to
   end tests run against a fixture that defines its own `Retry`.
3. The cpu decision in Rust only sees a type name in JSON read off the child's
   pipe. Class identity cannot cross a process boundary.

`_exec.py` once used `isinstance` and was changed to the name rule so all three
lanes agree.

## Decision

Keep name plus `countdown` matching, and document it for users
(docs/CONFIGURATION.md, "Task decorator").

The collision is close to harmless because cauli retries every exception by
default. A forced retry and a retryable failure go through the same path in
`worker/src/dispatch.rs`: the same `retries < max_retries` gate, the same
`schedule_retry`, the same dead letter with reason `max_retries`, and the same
stored failure carrying the user's own type, message and traceback. The only
difference for a user's own `Retry` is that its `countdown` replaces the
computed backoff. Someone writing such a class is most likely using the Celery
retry idiom, in which case that is what they meant.

## Consequences

- The shim rebinds its `SoftTimeLimitExceeded` from `sys.modules["cauli"]` once
  `load_app` has imported the app (`worker/src/shim.py`). Before this, a source
  built or `VIRTUAL_ENV` deployment bound a local stand in class, so a sync
  task's `except SoftTimeLimitExceeded` never matched the exception the worker
  injected. The task context names are rebound the same way.
- On the async lane the soft limit cancels the task, so the body sees
  `asyncio.CancelledError` and an `except SoftTimeLimitExceeded` inside it does
  not fire. The reported failure is still `SoftTimeLimitExceeded`.
- A non numeric `countdown` is ignored and the computed backoff is used, on both
  the io and cpu lanes. It no longer replaces the user's error with a
  `ValueError`.
- `ctx.rs` and `pyrt.rs` default `retryable` to false for a type named
  `SerializationError` when the response does not say. Both Python executors
  now stamp `retryable: true` on every user exception, so a user class named
  `SerializationError` retries on every lane. Only the worker's own
  `SerializationError`, for a result or arguments that cannot be encoded, is
  terminal.

## Alternatives considered

- `isinstance` first with a name fallback. Rejected: the fallback still catches
  every collision.
- A marker attribute on `cauli.Retry`. Rejected: a worker running against an
  older installed `cauli` whose `Retry` lacks the marker would silently degrade,
  and apps without `cauli` still need the name rule, so the collision remains.
