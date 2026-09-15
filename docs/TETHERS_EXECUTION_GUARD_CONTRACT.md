# Tethers execution-guard contract

This is the boundary between Resolve and Tethers in R0.

Resolve supplies a generic guard containing an identifier, claim epoch,
opaque scope information, and boot generation. Every guarded consequential
action must supply at least one real opaque authoritative `ScopeKey`, even if
the action is otherwise intended to be unrestricted; Resolve never substitutes
a dummy or global key. R0 models the Tethers admission
boundary with a store operation; a future Tethers adapter will validate that
guard at action admission against the action scope. Tethers does not inspect Goal,
Commitment, parent, dependency, or Resolve-plan concepts.

After admission, Tethers owns the consequential action until it reports one
authoritative outcome:

```text
SUCCEEDED | FAILED | UNCERTAIN
```

Resolve records the outcome and updates its live coordination state in one
transaction. `SUCCEEDED` and `FAILED` resolve the unique guard, clear the
outstanding action, and release every held scope lock. `UNCERTAIN` resolves
neither the action nor the locks: it marks the guard uncertain, retains the
outstanding action, and moves the commitment to
`WAITING(UncertainAction(action_ref))`. Resolve does not execute the action,
apply Tethers policy, or reinterpret `UNCERTAIN` as `FAILED`.

Resolve may issue the guard and reserve its scopes while the commitment is
`CLAIMED`, but admission requires the commitment to be explicitly `WORKING`
before it records the opaque action reference and exact scope match. Admission
does not start the commitment; policy and execution remain Tethers-owned. On a
real Resolve file-backed startup, active claims and issued reservations are
invalidated, while admitted action/held-lock pairs are reconstructed only when
their persisted relationship is internally consistent. Tethers must continue to
provide at least one opaque authoritative scope key for every guarded
consequential action.
