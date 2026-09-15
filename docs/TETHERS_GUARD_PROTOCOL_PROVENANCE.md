# Tethers Guard Protocol Provenance

Protocol: `resolve.tethers-guard/1`

Canonical protocol document: `docs/TETHERS_GUARD_PROTOCOL.md`

Authoritative Resolve protocol-source commit:

`70f86ff47cbfd38e702cdd8c1433ccd04ecb43bd`

This commit contains the canonical protocol document and is descended from the
accepted Resolve R0 `main` anchor `8d42e5b061f86b2b2a2c1949c629654968a550ff`.
It is the stable content SHA that Tethers pins; it remains fetchable from
Resolve `main` after the provenance PR is merged.

Tethers evidence compared against this contract:

- P3 implementation section head:
  `979057de8d1b6294dbcd87405f7a9fdcb2085b39`
- P3 merge on Tethers `main`:
  `63aa8e21bfa359ad444d8b32b3007b398fa9002e`
- Tethers P3 implementation:
  `tethers-0.1/host-rust/src/resolve_transport.rs`
- Accepted P3 transport tests and real Resolve/Firestore-emulator smoke are
  recorded in PR #36 and
  `docs/worker-notes/2026-09-15-tethers-resolve-p3-live-transport.md`.

Comparison date: 2026-09-16

Result: `MATCH`

The previously cited SHA
`b450305e72815d33357359c6db641d315d6b2975` is not retained as authority. It
was not recoverable from Resolve local or remote refs, tags, reflog, public
commit lookup, commit search, or Resolve pull-request history. It is therefore
classified as orphan provenance, not reconstructed or fabricated.

The canonical document above is reconstructed only from accepted R0 ownership
and outcome semantics, the merged Tethers P3 implementation and tests, the
accepted P3 architecture/worker evidence, and the recorded real Resolve smoke.
No transport semantic change is introduced by this closeout.
