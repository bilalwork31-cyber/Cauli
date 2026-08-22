# Security policy

## Supported versions

| Version | Supported |
|---|---|
| 1.0.x | yes |
| anything older | no. 0.1.0 was never published |

Both packages, `cauli` and `cauli-worker`, ship the same version number and are
covered by the same policy.

## Reporting a vulnerability

Report privately through GitHub's advisory form:
<https://github.com/bilalwork31-cyber/Cauli/security/advisories/new>. Please do
not open a public issue for a suspected vulnerability, and please do not post a
proof of concept anywhere public before a fix is out.

This is a small project run by one maintainer. What that means for you, stated
so you can plan around it rather than guess:

- Acknowledgement inside 7 days.
- A first assessment, including whether it is accepted, inside 14 days.
- No paid bounty, and no service level commitment beyond the two above.

Tell us the version of both packages, the Redis version, whether the worker is
the published wheel or a source build, and the smallest reproduction you have.

## What is in scope

cauli executes application code that arrives over a broker, so the interesting
boundary is what a message can make a worker do:

- Anything that makes a worker execute code the app did not register.
- Anything that escapes the JSON envelope: cauli never unpickles and resolves
  task names against a registry snapshot rather than importing by name, so a
  path around either is in scope.
- Anything reachable through the fork server socket, which is created at 0700
  and checks `SO_PEERCRED`.
- Credential leaks: the Redis URL is redacted in every log path and is passed
  to supervised processes through the environment rather than argv.
- Memory safety in the Rust worker, and anything that crashes it from a crafted
  envelope rather than dead lettering the envelope.

## What is not in scope

These are documented design positions, not oversights. Reporting one gets the
link to this section.

- **Redis is trusted.** Anything with write access to the broker can enqueue
  any registered task with any arguments. Run Redis on a private network with
  `requirepass` or `rediss://`, and treat broker access as equivalent to code
  execution in your workers.
- **Task arguments, results and tracebacks are stored in Redis in plaintext.**
  Keep secrets out of all three.
- **Dead letter entries hold the full envelope, arguments included, with no
  expiry.** They are capped at roughly 1000 entries per queue by count only.
- **Idempotency keys are folded through 64 bit FNV-1a**, which is not a
  cryptographic hash. A caller who can choose keys can construct a collision
  and suppress a distinct task. Do not derive idempotency keys from untrusted
  input. This is a known 1.0 gap, tracked for a 128 bit replacement, and a
  report of it will be closed as already known rather than as invalid.
- **A task body can do anything the worker process can.** cauli runs your code;
  it is not a sandbox.
