# Error taxonomy in the result document

Status: Accepted (0.1.0)

## Context

A failed task's result document carries an error object with `type`, `message`
and `traceback` (PROTOCOL.md section 8). Before 0.1.0 a caller could not tell
an error cauli synthesized from an exception the task raised, and the string
`TimeoutError` meant three different things: the caller stopped waiting, the
worker killed the task at its limit, or the task raised the builtin itself.

The error vocabulary becomes expensive to change once released, so the shape
had to be settled before the first release.

## Decision

- Add `error.origin` to the error object (PROTOCOL.md section 8.1). `"worker"`
  means cauli machinery synthesized the error object. `"task"` means an
  exception propagated out of user code. `"client"` is reserved for errors the
  client synthesizes locally, today only `InvalidResult`, and never appears on
  the wire.
- A `SoftTimeLimitExceeded` that propagates carries origin `"task"`. The worker
  injected it, but it left user code, and a task may catch it and return
  normally. This is documented, not special cased.
- Rename the worker minted `TimeoutError` to `TimeLimitExceeded` (PROTOCOL.md
  section 8.2). The three meanings now have three spellings: the builtin
  `TimeoutError` from `.get(timeout=)` means the caller gave up waiting;
  `TimeLimitExceeded` with origin `"worker"` means the worker killed the task;
  `TimeoutError` with origin `"task"` means the task raised it. The name also
  pairs with `SoftTimeLimitExceeded`.
- State in PROTOCOL.md section 8 that clients must ignore unknown fields. That
  rule is what allows the result document to grow additively later.
- Publish "did the task ever run" as a table derived from the closed set of
  worker minted types (PROTOCOL.md section 8.3), rather than adding a field.
  `Malformed`, `UnregisteredTask` and `Expired` never ran. `SerializationError`
  ran to completion but its result was lost. Everything else has unknown side
  effects.

Everything else is frozen: the other sentinel type strings stay as they are,
the dead letter `reason` values stay a separate snake case namespace, and
`expired` stays its own status.

## Consequences

- `origin` is additive. A client reading a document from an older worker sees no
  `origin` key and must treat it as unknown. `AsyncResult` reads with `.get()`
  and never validates the key set, so nothing broke.
- The rename touched only the worker's mint sites in `worker/src/exec.rs`, the
  tests that asserted the old string, and PROTOCOL.md. No Python code matched on
  the string.
- Result documents carry no `retryable` flag or retry count. The worker knows
  whether a failure was retryable, but a caller can only tell exhausted retries
  from a non retryable failure through the dead letter `reason`. Adding the flag
  later is additive.
- The original exception class is reduced to its name. `ProtocolError` from a
  cpu child is logged only and never becomes a result.

## Alternatives considered

- A string prefix such as `cauli.Malformed` instead of an `origin` field.
  Rejected: it rewrites every documented type string to encode one bit.
- An exception hierarchy on the client, or rehydrating the original exception
  type. Rejected: the class must be importable and constructible on the client,
  pickle based rehydration means executing code read from Redis, and class
  identity is already unreliable across the embedded interpreter boundary (see
  [retry-name-matching.md](retry-name-matching.md)).
