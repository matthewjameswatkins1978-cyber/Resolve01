# Tethers Guard Protocol v1

Protocol identifier: `resolve.tethers-guard/1`

This document is the durable Resolve-side description of the already accepted
Tethers P3 bridge behaviour. It defines the narrow coordination boundary only.
Resolve owns live guard and lock truth. Tethers owns permission, execution and
provider outcome truth. Resolve admission cannot grant Tethers permission.

## Transport boundary

The bridge uses synchronous authenticated HTTPS requests. The two supported
operations are:

```text
POST /internal/tethers/v1/guard/admit
POST /internal/tethers/v1/outcome
```

The client sends `Content-Type: application/json` and the explicit bridge
authentication header `X-Resolve-Tethers-Key`. The key is host configuration,
not Tether source, provider, action arguments, or Resolve state. The client
does not send cookies or use ambient proxy configuration.

Production configuration requires an `https://` base URL. HTTP is accepted only
by the explicit local-test constructor for loopback hosts. The client accepts a
timeout from 100 ms through 30 s, defaulting to 5 s. Request and response
bodies are each limited to 16 KiB. The client follows no redirects and accepts
only HTTP status 200. Transport errors, authentication failures, non-success
statuses, and response-body failures are closed transport failures.

## Shared encoding and validation

Requests and responses are UTF-8 JSON objects. JSON parsing rejects malformed
JSON and duplicate object members. Each response operation requires its exact
closed object shape; unknown or missing members are rejected. The protocol
version must be exactly `resolve.tethers-guard/1`.

Digests use the existing Tethers canonical JSON/SHA-256 authority and are
encoded as `sha256:` followed by 64 lowercase hexadecimal characters. A
response digest is accepted only when it is valid and equals the independently
recomputed digest of the complete request object. A digest mismatch is a
closed protocol failure.

Scope keys are opaque digest-shaped values. Tethers supplies them in strict
ascending order without duplicates, with at most 64 keys. Resolve does not
interpret paths, hierarchy, or resource meaning.

## Guard admission

### Request

The request body has exactly these members:

```json
{
  "protocol_version": "resolve.tethers-guard/1",
  "guard_ref": "sha256:<64 lowercase hex characters>",
  "action_id": "<opaque host-created action reference>",
  "preparation_digest": "sha256:<64 lowercase hex characters>",
  "scope_keys": [
    "sha256:<64 lowercase hex characters>"
  ]
}
```

The accepted P3 wire member is named `action_id`. Its value is the opaque
Tethers action reference derived from the host-created durable `ExecutionId`;
it is not the planner `ActionId`. The preparation digest is the current P1
preparation identity. The guard reference and ScopeKeys are opaque values; no
Goal, Commitment, Claim, worker, policy, arguments, paths, manifest, provider
payload, or Resolve state crosses this boundary.

The action reference is non-empty, at most 256 bytes, and limited to ASCII
letters, digits, `.`, `_`, `:`, `/`, and `-`. The guard and preparation
digests and every ScopeKey must satisfy the digest encoding above.

The response body has exactly these members:

```json
{
  "protocol_version": "resolve.tethers-guard/1",
  "request_digest": "sha256:<64 lowercase hex characters>",
  "decision": "ADMITTED | REJECTED | INDETERMINATE",
  "reason_code": "<closed reason code>"
}
```

The closed `reason_code` vocabulary is:

```text
admitted
guard_not_found
guard_revoked
guard_already_bound
guard_expired
task_not_active
ownership_changed
fence_changed
action_mismatch
preparation_mismatch
scope_keys_mismatch
state_unavailable
internal_integrity
```

`ADMITTED` is valid only with `admitted`. `REJECTED` and `INDETERMINATE`
require any listed reason other than `admitted`. Unknown decisions, reason
codes, protocol versions, fields, or digest values fail closed. A valid
`REJECTED` or `INDETERMINATE` response never invokes a provider.

## Outcome delivery

Outcome delivery reports an outcome already classified and made durable by
Tethers. It does not execute or retry a provider.

### Request

The request body has exactly these members:

```json
{
  "protocol_version": "resolve.tethers-guard/1",
  "action_id": "<opaque host-created action reference>",
  "preparation_digest": "sha256:<64 lowercase hex characters>",
  "outcome": "SUCCEEDED | FAILED | UNCERTAIN"
}
```

The `action_id` has the same opaque host-created action-reference meaning as
in admission. The preparation digest is retained so the delivery is bound to
the same accepted preparation identity. The outcome vocabulary is closed:
`SUCCEEDED`, `FAILED`, and `UNCERTAIN`. Tethers does not send contradictory
outcomes as a correction.

The response body has exactly these members:

```json
{
  "protocol_version": "resolve.tethers-guard/1",
  "delivery_digest": "sha256:<64 lowercase hex characters>",
  "result": "RECORDED | ALREADY_RECORDED | CONFLICT"
}
```

`RECORDED` confirms a newly recorded matching outcome. `ALREADY_RECORDED`
confirms an exact duplicate and is a successful idempotent acknowledgement.
`CONFLICT` is a distinct acknowledgement for the same action/preparation
identity with a contradictory outcome; it is not permission to rewrite the
Tethers result and is not converted into a provider outcome.

## Security and failure boundary

The bridge key is required, is sent only in `X-Resolve-Tethers-Key`, and is
redacted from client debug/display output, errors, Trail evidence, and
verification evidence. Missing or invalid configuration prevents bridge use.

The client rejects unsupported protocol versions, malformed or duplicate JSON,
unknown fields, invalid UTF-8, oversized bodies, invalid digests, digest
mismatches, unknown decisions/results, and non-success HTTP responses. Redirect
and ambient-proxy use are disabled. These failures cannot grant admission or
alter provider execution truth.

This protocol has no retry engine, scheduler, Resolve database access, public
Resolve Plug, provider retry, or automatic outcome reconciliation. Explicit
outcome redelivery is permitted only for an already durable Tethers outcome
because exact duplicate delivery is idempotent at this boundary.

## Authority statement

> Resolve owns live guard/lock truth. Tethers owns permission, execution and
> provider outcome truth.

> Resolve admission cannot grant Tethers permission.

