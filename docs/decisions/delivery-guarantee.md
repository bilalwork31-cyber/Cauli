# Delivery guarantee

Status: Accepted (0.1.0)

## Context

Delivery behaviour was spread over several sections of PROTOCOL.md and stated
differently in the README and CONFIGURATION.md. Two problems stood out.

- The worst way to lose a task, an enqueue the Redis master accepted and never
  replicated before a failover, was documented only inside the beat chapter
  (PROTOCOL.md section 10.5).
- The documentation offered `idempotency_key` as an alternative to writing a
  task that is safe to repeat. It is not one.

The idempotency guard also had two defects. Its claim used `idemp_ttl` as is, so
a task whose timeout was longer than `idemp_ttl` could lose its claim while
still running, and the next attempt would claim it again and run concurrently.
And a suppressed duplicate gave the caller no way to find the task that held
the key.

## Decision

The guarantee is stated once, in PROTOCOL.md section 4 under "Delivery
guarantee":

- Once Redis has accepted an enqueue, a task is never lost silently. It either
  runs to a recorded outcome or lands in the dead letter stream with a reason.
- Execution is at least once. Every internal failure (a truncated completion
  pipeline, a worker crash, a script error, a failed idempotency check)
  resolves toward running the task again, never toward dropping it.
- Work terminates. A task gets `max_retries` failed executions, and each attempt
  at most `max(3, max_retries + 1)` crash redeliveries
  (`envelope::redelivery_limit`). Each dead letter stream is capped at about
  1000 entries and expires 7 days after its last write
  (`DLQ_MAXLEN` and `DLQ_TTL_S` in `worker/src/broker.rs`).
- All of this holds within one Redis dataset. An asynchronous replication
  failover can forget writes that were not replicated. That is the one way a
  task can vanish or a beat slot can fire twice.

The user's obligation follows from it: every task must tolerate running twice.
`idempotency_key` narrows the window and does not remove the obligation.

The idempotency guard is an atomic execution admission lease
(`IDEMP_CLAIM_LUA` in `worker/src/broker.rs`):

- `SET NX EX` claims the key. If the holder is the same task id (a retry or a
  crash redelivery), the claim is renewed with `PEXPIRE` and execution proceeds.
- The claim TTL is `max(idemp_ttl, timeout_ms + backstop grace)`
  (`broker::claim_ttl_s`), so a claim cannot expire while its own attempt runs.
- A duplicate result carries the claimant's task id. `AsyncResult.claimant_id`
  exposes it and `AsyncResult.claimant()` returns a handle to the claimant's
  result.
- Claims are never released, not even after the claimant is dead lettered.

## Consequences

- After the claimant has been dead lettered, a resubmission with the same key is
  suppressed until the claim expires. This is the one behaviour that leans
  toward loss rather than duplication. It is kept because a failed task may
  have left partial side effects, and the claimant id lets the caller discover
  the real outcome.
- The guard fails open: if the claim script errors, the task runs anyway.
- Retries and crash redeliveries are counted separately, so the worst case is
  `(max_retries + 1) x (redelivery_limit + 1)` executions. Anyone sizing work on
  the retry count needs that product.
- Work that must never run twice needs its own check inside the task, keyed on
  something stable, such as `beat_slot` for scheduled work.
- Operators should keep `--visibility-timeout` above the longest task timeout,
  watch the dead letter streams before the cap drops old entries, and expect a
  failover to duplicate recent work. The worker warns at startup about task
  timeouts at or above the visibility timeout or `idemp_ttl`.
- The MineAgain path under crash redelivery is covered by the integration suite
  (`itest/test_integration.py`).

## Alternatives considered

- Release the claim when the claimant fails terminally. Rejected: partial side
  effects make suppression the safer default.
- Describe the guard as exactly once. Rejected: it does not survive a failover
  and fails open on error.
