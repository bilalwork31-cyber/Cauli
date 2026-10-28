# Worker clock anchored on Redis TIME

Status: Accepted (0.1.0)

## Context

Every duration in the worker is measured with a monotonic clock. Absolute
instants are different: several of them are written by one process and read by
another, so they only work if every process means the same thing by "now".

- The delayed sorted set (PROTOCOL.md section 4.3) has several writers (the
  client, retries on every worker, beat) and a reader, the mover, on every
  worker. Because every worker runs the mover, the fastest local clock in the
  fleet decides when delayed work fires. One worker stepped forward fires all
  delayed work early and defeats backoff and `eta`.
- A forward step during `schedule_retry` writes a score far in the future, and
  nothing ever corrects it.
- The expiry check (PROTOCOL.md section 9.1) compares the worker's clock with a
  deadline the client stamped. A worker ahead of the client drops valid work as
  expired, which is the worst direction to be wrong in.

## Decision

The worker reads absolute time from a sampled, Redis anchored clock
(`worker/src/clock.rs`).

- `now_ms()` is a stored anchor plus monotonic time elapsed since the process
  started. Between samples the reading never steps and never goes backwards.
- The first `TIME` sample is taken, blocking, at startup. The worker already has
  to reach Redis to create its consumer groups, so this costs nothing extra.
- A background task resamples every 15 to 30 seconds. The exact period is
  spread by pid so a fleet that booted together does not hit Redis in step.
- If `TIME` fails at startup (some managed Redis services deny it by ACL), the
  worker falls back to its local clock, warns, and keeps sampling. A clock that
  has not been sampled for 90 seconds is logged once as stale.
- At startup the worker warns when the local clock and Redis disagree by one
  second or more, since log timestamps and anything enqueued from that host are
  off by the same amount.
- The retry delay is clamped to 30 days (`MAX_RETRY_DELAY_MS` in
  `worker/src/dispatch.rs`), mirroring the 24 hour clamp on `timeout_ms`. Both
  inputs to the delay, `backoff_max_ms` from the envelope and
  `Retry(countdown=...)` from the task, are otherwise unbounded.
- `cauli-beat` reads "now" from Redis `TIME` as well (`py/cauli/beat.py`).

## Consequences

- An NTP step on one worker host has no effect on scheduling. All workers and
  beat agree with Redis to within the drift between two samples.
- No wire change was needed. Scores and stamps keep their meaning.
- The Python client still stamps `enqueued_at`, numeric `expires` and
  `countdown` from its local clock. `eta` and a datetime `expires` are absolute
  values the user supplies, so they do not depend on the client clock. A skewed
  client mostly misplaces its own tasks, by the size of its skew. Client hosts
  need NTP.
- An entry's pending entries list idle time starts at delivery, but its
  execution timer only starts once it holds an io permit, or for cpu tasks once
  a child picks it up. Under saturation that wait is unbounded, so a parked
  entry could be reclaimed by recovery and dead lettered as `redelivery_limit`
  without ever running. The fetch loop therefore requests no more entries than
  there are free io permits, divided across the streams it reads, and the
  recovery loop reclaims no more than the free permit count
  (`worker/src/loops.rs`). The cpu path is still partly exposed through the cpu
  backlog. That is accepted: a reclaim resets the idle time, which limits the
  effect.
- Scores already written with a bad clock are never rewritten, and the mover
  does not detect them.

## Alternatives considered

- Read Redis `TIME` on every call. Rejected: it adds two serial round trips per
  task (the expiry check and the finish stamp) to buy a timestamp.
- Cap retries with a horizon such as `fire_at < now + horizon`. Rejected: both
  sides of the comparison use the same broken clock, so a local clock fault
  passes the check. Clamping the delay instead survives.
- A Redis anchored clock in the Python client, a hybrid logical clock, or any
  other wire change. Rejected as more than the problem needs.
- Re arming execution timers at delivery. Rejected: it would change timeout
  semantics for queued work to close a window that at least once delivery
  already covers.
