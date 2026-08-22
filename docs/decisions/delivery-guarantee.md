# Decision: the delivery guarantee at 1.0
> **Historical design note, not current documentation.** This is a record of how one
> pre 1.0 decision was reached and what was known when it was reached. It is kept
> because the reasoning is worth reading, not because it describes today's behaviour.
> Where it disagrees with the code, with [PROTOCOL.md](../../PROTOCOL.md) or with
> [docs/CONFIGURATION.md](../CONFIGURATION.md), those win. The status line below was
> checked against the source, not carried over.
>
> **Status: shipped in 1.0.0.** The idempotency claim TTL now
> derives from the execution it guards (`broker::claim_ttl_s`), the worker carries the
> claimant id back on a suppressed duplicate, PROTOCOL.md section 4 carries the delivery
> guarantee preamble, and README no longer offers an idempotency key as an alternative to
> writing a repeatable task body. The last piece landed in the release audit pass:
> `AsyncResult.claimant_id` now carries the claiming task id after a duplicate resolves, and
> `AsyncResult.claimant()` hands back a result handle for it, so a suppressed caller can read
> the outcome that actually ran.

**The guarantee is coherent, not a pile of caveats.** Every cauli side failure resolves the same
direction: toward duplicate execution or a loud dead letter, never toward silent loss. That composes.
What does not compose is the documentation, plus one component whose name promises more than its
mechanism delivers.

## The guarantee, stated as it actually is

Once Redis has accepted an enqueue, cauli never loses the task silently: it either executes to a
recorded outcome or lands in the dead letter stream with a stated reason. Execution is at least once.
Every internal failure, a truncated completion pipeline, a worker crash, a mid script error, a failed
idempotency check, resolves toward running the task again rather than dropping it, so duplicates are
always possible; `idempotency_key` suppresses most of them for `idemp_ttl` seconds, best effort. Work
terminates within bounds: `max_retries` failed executions, at most `max(3, max_retries + 1)` crash
redeliveries per attempt, then a dead letter queue capped at roughly 1000 entries per queue. Beat
fires each slot at most once per surviving Redis dataset. All of this is scoped to ONE Redis dataset:
an async replication failover can forget unreplicated writes, which is the one place a task can
vanish or a beat slot can fire twice, and delayed, retried and periodic tasks do not work on Cluster.

## What a user must do

Write every task to tolerate running twice, unconditionally. `idempotency_key` narrows the window; it
does not remove that obligation. Work that truly must not run twice needs its own dedup check inside
the task, keyed on something stable, `beat_slot` for scheduled work. Operationally: keep
`--visibility-timeout` above the longest task timeout and `idemp_ttl` above the longest run plus
retry horizon, both of which the worker now warns about, watch the dead letter queue before its cap
rotates, and run standalone or Sentinel knowing a failover can duplicate recent work.

## The new finding: the idempotency claim fails CLOSED after permanent failure

This is the one caveat that resolves in the LOSS direction, and it is undocumented. Nothing ever
deletes a claim. After the claimant has been dead lettered, a resubmission with the same key returns
"duplicate" until the TTL runs out, and the duplicate result carries no claimant id, so the caller
cannot even discover that the work never succeeded.

Orchestrator position: do NOT release claims on terminal failure, because partial side effects make
suppression the safer default. But the caller being unable to find out is a real bug. Write the
claimant task id into the duplicate result so a suppressed caller can chase the actual outcome.

## What the guard actually is

An atomic execution admission lease, not an idempotency guarantee. It kills the common double submit
and concurrent dedup races. It also: fails open on a corrupted key; anchors its window at first claim
and never refreshes it; shares the failover lost write window; and still has NO test for the crash
redelivery half of its MineAgain behaviour. Keep it, rename its story.

## The weakest point is documented at the wrong address

The weakest point is the Redis durability boundary, an enqueue the master acked and never replicated:
the only failure that silently loses a task and the only one cauli cannot route to a dead letter. It
IS documented, but only inside the beat chapter at section 10.5. Section 4 and the README's shipping
list never mention failover, and instead point loudly at Cluster and visibility timeout, which are
the well covered spots.

## The two changes

1. **Code, under 20 lines.** Derive the claim TTL from the execution it guards: claim with
   `EX max(idemp_ttl, (timeout_ms + grace)/1000)` and add a PEXPIRE refresh to the MineAgain branch
   of the Lua. That turns tonight's startup warning into an invariant, since a claim can then never
   expire while its own execution or retry chain may still run. Plus the claimant id in the duplicate
   result, above.

2. **Documentation, and this matters more.** Add a "Delivery guarantee" preamble to PROTOCOL section
   4 carrying the two paragraphs above verbatim, and move the per Redis dataset failover caveat there
   from 10.5.

   Then fix README line 266, which currently says make tasks safe to repeat **or** pass an
   `idempotency_key`. **That "or" is false** and it is the single most misleading sentence in the
   documentation: the key never substitutes for repeat safety. Same correction to CONFIGURATION.md's
   "Deduplicates execution" row.

Also unstated anywhere: worst case executions are `(max_retries + 1) x (redelivery_limit + 1)`,
because the two counters are deliberately disjoint. A user sizing anything on retry count needs that
product.
