# Decision: error taxonomy at 1.0
> **Historical design note, not current documentation.** This is a record of how one
> pre 1.0 decision was reached and what was known when it was reached. It is kept
> because the reasoning is worth reading, not because it describes today's behaviour.
> Where it disagrees with the code, with [PROTOCOL.md](../../PROTOCOL.md) or with
> [docs/CONFIGURATION.md](../CONFIGURATION.md), those win. The status line below was
> checked against the source, not carried over.
>
> **Status: shipped in 1.0.0.** `error.origin` is on the wire (PROTOCOL.md section 8) and
> the worker minted `TimeoutError` was renamed `TimeLimitExceeded`. Everything else in the
> taxonomy is frozen for 1.x.

**Recommendation: add one additive wire field, rename one string, freeze everything else. Both before
1.0, because the rename is only cheap now.**

### The two changes

1. **Add `error.origin` to the result document**, valued `"task"` or `"worker"`, with `"client"`
   reserved for client synthesized errors such as `InvalidResult`. The definition is mechanical so it
   cannot drift: "worker" means cauli machinery synthesized the error object, "task" means an
   exception propagated out of user code.

   Chosen as a field rather than a string prefix such as `cauli.Malformed`, because a prefix rewrites
   all 12 documented strings in order to encode one bit, while the field breaks nothing: `result.py`
   reads with `.get()` and ignores unknown keys.

2. **Rename the worker minted `TimeoutError` to `TimeLimitExceeded`.** It is symmetric with the
   existing `SoftTimeLimitExceeded` and it stops shadowing a Python builtin. The three meanings then
   get three spellings: the builtin `TimeoutError` means the CALLER gave up waiting;
   `TimeLimitExceeded` with origin worker means the worker killed it; `TimeoutError` with origin task
   means the task raised its own. `.get(timeout=)` keeps raising the builtin, which is the Python
   idiom for a local wait.

   One documented edge: a propagated `SoftTimeLimitExceeded` carries origin "task", because it did
   leave user code. Document it, do not special case it.

### What is deliberately NOT changing

The other 10 sentinel strings stay verbatim. The dead letter `reason` axis stays a separate snake case
namespace. `expired` remains its own status. No `retryable` flag or retry count is added to result
documents, since the dead letter entry already records it and it can be added later additively. No
exception hierarchy. `InvalidResult` stays client side.

**The original exception type stays flattened to a name.** Rehydrating it would need the class both
importable and constructible on the client, and pickle based rehydration is code execution from Redis.
This codebase already demonstrates that class identity across the embedded boundary is unreliable,
which is precisely why `Retry` is matched by name. Celery parity is correct here.

### The sentence that matters most

Add to PROTOCOL section 8: **clients must ignore unknown fields.** That single sentence is what makes
any post 1.0 evolution of this document possible at all, and it costs nothing now.

### Branching table, derived rather than added

The "did it ever run" axis a caller most needs is fully derivable from the closed sentinel set and
should be published as a table in section 8 rather than encoded as another field: never ran, for
`Malformed`, `UnregisteredTask` and `Expired`; ran to completion but the result was lost, for
`SerializationError`; side effects unknown, for the rest.

### Blast radius, measured not estimated

The additive field: zero breakage, and a new client against an old worker sees `None` and treats it as
unknown. The rename: 3 mint sites in exec.rs, 11 test assertions across 3 e2e files, about 4 lines of
PROTOCOL.md, and zero Python matchers. No compatibility flag needed, one changelog line.

### Corrections this review made to my own briefing

I had told it there were three live JSON error paths and several other things; it verified against
source first and sharpened three: `retryable` DOES exist internally but is dropped from the result
document, so callers cannot distinguish exhausted retries from never retryable except through the
dead letter reason; `InvalidResult` is client only and never appears on the wire; and `ProtocolError`
never becomes a result at all, it is log only.

### A new finding it surfaced, now under separate investigation

`shim.py` around line 148 treats ANY user class named `Retry` carrying a `countdown` attribute as a
forced retry, matched by NAME rather than identity, and the same duck typed rule appears in
`_exec.py` and `ctx.rs`. So all three lanes agree with each other, which means they would agree on
the wrong thing too. A user defining their own `Retry` class, which is not an exotic name, could have
a real application error silently swallowed and the task rescheduled. Being investigated separately,
including why identity matching was not used, since the answer determines the fix.

