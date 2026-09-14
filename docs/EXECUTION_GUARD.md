# Execution guard

An `ExecutionGuard` is a Resolve-issued, opaque coordination token. It proves
only that Resolve currently recognises a worker as the fenced owner of a
commitment and its exact opaque scope keys. It is not a permission grant and
contains no Tethers policy.

The R0 lifecycle is:

```text
ISSUED -> ADMITTED(action_ref) -> terminal outcome
       \-> INVALIDATED
```

Issuance reserves exact scope keys. Admission promotes those reservations to
held locks atomically with the outstanding action reference. Held locks are
not released by worker lease expiry or guard-token expiry.

S0 establishes the documented boundary only. Guard mechanics are delivered in
S3, after the domain and persistence sections have been merged.
