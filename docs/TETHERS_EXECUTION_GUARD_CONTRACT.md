# Tethers execution-guard contract

This is the boundary between Resolve and Tethers in R0.

Resolve supplies a generic guard containing an identifier, claim epoch,
opaque scope information, and boot generation. Every guarded consequential
action must supply at least one real opaque authoritative `ScopeKey`, even if
the action is otherwise intended to be unrestricted; Resolve never substitutes
a dummy or global key. S3 models the Tethers admission
boundary with a store operation; a future Tethers adapter will validate that
guard at action admission against the action scope. Tethers does not inspect Goal,
Commitment, parent, dependency, or Resolve-plan concepts.

After admission, Tethers owns the consequential action until it reports one
authoritative outcome:

```text
SUCCEEDED | FAILED | UNCERTAIN
```

Resolve records the outcome and updates its live coordination state. It does
not execute the action, apply Tethers policy, or reinterpret `UNCERTAIN` as
`FAILED`.

S3 has no Tethers integration or execution capability. Resolve may issue the
guard and reserve its scopes while the commitment is `CLAIMED`, but the mock
admission path requires the commitment to be explicitly `WORKING` before it
records the opaque action reference and exact scope match. Admission does not
start the commitment; policy and execution remain Tethers-owned.
