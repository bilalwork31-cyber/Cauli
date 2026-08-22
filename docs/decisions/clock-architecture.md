# Decision: time and clock architecture at 1.0
> **Historical design note, not current documentation.** This is a record of how one
> pre 1.0 decision was reached and what was known when it was reached. It is kept
> because the reasoning is worth reading, not because it describes today's behaviour.
> Where it disagrees with the code, with [PROTOCOL.md](../../PROTOCOL.md) or with
> [docs/CONFIGURATION.md](../CONFIGURATION.md), those win. The status line below was
> checked against the source, not carried over.
>
> **Status: shipped in 1.0.0.** `worker/src/clock.rs` anchors on Redis `TIME`, re anchors
> on a periodic sample and warns at boot about a skew worth naming. The Python client is
> still on its own clock, which CHANGELOG's known limitations now says plainly.

**Ship 1.0 with a Redis anchored SAMPLED clock in the worker. Do not route per call through Redis
`TIME`.** The mixed clock shared timeline is a genuine 1.0 defect, but the correct fix is small.

## It corrected the audit twice, and one correction matters a lot

First, my briefing cited `ctx.rs:98-103`; that is now `DecrGuard`, and the real site is `ctx.rs:111`.

Second, and this is the important one: **the recovery versus backstop window is LARGER than the audit
judged, and in one regime it is a real defect rather than a curiosity.** The audit concluded it was
"swamped in practice by tick granularity". That is true only on an idle system. The timer is armed
AFTER the io semaphore is acquired (exec.rs:45 and :106) and, for cpu jobs, only at child pickup
(`arm_started` in cpu.rs). That wait is UNBOUNDED under saturation.

So under load: an entry parked past `timeout_ms + 2000` of idle gets reclaimed while its attempt is
still alive and has not started; repeated parking inflates `delivery_count`; and about three cycles
reach the redelivery dead letter **without the task ever executing once**. A task dead lettered
having never run is exactly the class of bug this audit existed to find, and it was mis-triaged.

| regime | window | consequence | action |
|--------|--------|-------------|--------|
| idle | spawn plus parse plus one idemp round trip, single digit ms | one at least once duplicate, probability about window over tick | document, do not fix |
| saturated with long tasks | io semaphore or cpu backlog wait, unbounded | dead lettered without ever executing | REAL DEFECT, fix or measure before 1.0 |

Cheap fix for the io half: fetch `COUNT = min(batch, available_permits)` at loops.rs:35, so fetched
entries never park on the semaphore. About 5 lines. The cpu half stays partly exposed through the
backlog channel; accept and document, since XCLAIM resets idle and self limits it.

## The horizon cap I proposed does not work

Worth recording because it was my idea and it is wrong: `fire_at = bogus_now + backoff` passes any
check shaped like `fire_at < bogus_now + horizon`, because both sides carry the same broken clock. A
cap measured against the local clock cannot catch a local clock fault.

What survives is clamping the retry DELAY rather than the instant: `d_ms` at dispatch.rs:247 comes
from envelope controlled `backoff_max_ms` and task controlled `Retry(countdown=...)`, both unbounded.
Clamp at 30 days, mirroring the existing `MAX_TIMEOUT_MS`. Ten lines, worth doing regardless.

## Which timestamps are in the wrong category

Every DURATION in the codebase is already correctly monotonic. Only absolute instants are broken, and
only on the worker side. The delayed sorted set has three writers on two different clocks and one
reader on a third.

Concretely wrong, all fixed at once by the sampled clock: the mover cutoff, where since every worker
runs the mover the FASTEST local clock in the fleet defines firing, so one forward stepped worker
fires all delayed work early and defeats backoff and eta; the retry write, where a forward step
strands the task with no self healing; and the expiry check, where a worker ahead of the client
silently drops valid work as expired, which is the worst direction.

## The design

`RedisClock`: one `AtomicI64` offset, `now_ms() = offset + monotonic_elapsed_since_start`. A
background task samples Redis `TIME` every 15 to 30 seconds. Block once at startup for the first
sample, which is free because the worker already requires Redis at boot for `ensure_groups`. On
sample failure keep extrapolating monotonically and warn when stale.

Local NTP steps then have zero effect anywhere, all workers agree with beat and with Redis within a
few ms, and the pre epoch branch dies. Quartz drift between samples is about 3ms per minute, which is
irrelevant at 250ms mover granularity. No wire change. About 150 lines.

Per call `TIME` was rejected on measured grounds: it would add roughly 2 SERIAL round trips per task
at dispatch.rs:104 and :181, roughly doubling broker command load and directly attacking the measured
dispatch overhead claim.

The client stays on its local clock at 1.0. Its remaining exposure is only `enqueued_at`, numeric
`expires` and `countdown`; `eta` and datetime `expires` are user supplied absolutes and clock free.
A skewed client mostly hurts its own tasks, by a bounded amount. Document the NTP requirement.

## Ranked

1. `RedisClock` sampled offset, about 150 lines, low risk, no wire change
2. Clamp retry `d_ms` at 30 days, about 10 lines, no risk
3. PROTOCOL prose on clock requirements and recovery reference points
4. `COUNT = min(batch, permits)`, about 5 lines, closes the saturated duplicate window
5. Stranded score warning in the mover, detection only, never rewrite scores

Explicitly not doing: per call Redis TIME; a client side Redis clock at 1.0; HLC, Lamport or any wire
change; rewriting existing delayed scores; or re anchoring executor timers at delivery, which would
change timeout semantics for queued work to close a window duplicates already cover.
