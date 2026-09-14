# Tethers execution-guard contract

This is the boundary between Resolve and Tethers in R0.

Resolve supplies a generic guard containing an identifier, claim epoch,
opaque scope information, and boot generation. Tethers validates that guard
at action admission against the action scope. Tethers does not inspect Goal,
Commitment, parent, dependency, or Resolve-plan concepts.

After admission, Tethers owns the consequential action until it reports one
authoritative outcome:

```text
SUCCEEDED | FAILED | UNCERTAIN
```

Resolve records the outcome and updates its live coordination state. It does
not execute the action, apply Tethers policy, or reinterpret `UNCERTAIN` as
`FAILED`.

S0 documents this contract only. No Tethers integration or execution
capability is present in the bootstrap workspace.
