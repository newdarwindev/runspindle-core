# Explicit Tether conflict-resolution bridge

This additive lane addresses the integrated resolution gap left by the initial
wire adapter. It does not relax the normal transport's sticky-conflict behavior.
`ResolutionBridge` is deliberately **not** a public `Transport`: callers cannot
pass resolver evidence into ordinary outbox reconciliation to evade the separate
current `ResolutionAuthority`.

## Bound decision and independent observation

The distinct protocol is `runspindle.tether-wire-resolution-bridge.v1`, with
`resolve` and `lookup_resolution` actions. Both bind the exact accepted artifact
bytes, original delivery envelope, presentation payload and canonical decision.
The decision carries `runspindle.wire-resolution.v1`, a decision ID, policy ref,
authorization ref, SHA-256 of the original Tether conflict snapshot, SHA-256 of the
complete existing Core `ConflictResolution`, and selected file/message identities.
Core verifies the selected receipt is the hash of that exact provider pair.

Core accepts only identifier refs of at most 128 ASCII identifier bytes, a
`F[A-Z0-9]+` file ID (also at most 128), and a numeric Slack timestamp with six
fractional digits and at most 64 bytes. This deliberately narrower intersection
rejects Tether-only longer references before any peer call.

The response must bind the full request digest and canonical decision digest,
state `independently_reconciled_under_decision`, evidence class
`slack_wire_resolution_observation`, and the exact selected IDs. Unknown or any
mismatch is not delivery. A successful `resolve` reply is followed by a separate
fresh `lookup_resolution` inside Core's existing authority/transaction fences.
`lookup_resolution` is provider-read-only, but may idempotently retain the exact
same append-only local decision. It is not described as a read-only local-store
operation. Neither action allocates, uploads, completes, retargets or retries a
provider delivery.

Core and Tether each require their own current dedicated resolver authority;
ordinary delivery authorization is insufficient. Test-only fixture authorization
is supplied from independent expected files, not inferred from incoming JSON.
There remains no production credential, endpoint, default subprocess or identity
issuer. The bridge's request hashes are binding checks, not authentication.

## Crash recovery and bounded lifecycle

Core's explicit resolution is now idempotent for the **same retained decision**.
It requires the original generation plus one, exact selected receipt and exact
immutable resolution-audit record. A matching receipt alone is insufficient.
Every repeat still checks current dual authority, fresh provider observation and
the current snapshot after acquiring the write lock. This supports lost Tether
replies, a crash after Core commit, and competing processes without creating a
second decision or resending bytes.

Original Tether conflict rows are never cleared or reset. The separate decision
journal authorizes this explicit lane only. Ordinary `WireTransport.lookup`
continues to report sticky conflict and can re-quarantine Core after a successful
explicit resolution. The old decision cannot then authorize a new Core generation.
Tether permits one immutable decision per operation/conflict snapshot; a different
superseding decision or unbounded repeated resolution lifecycle is not implemented.
Use the explicit resolved lane to reobserve the qualified decision, rather than
silently falling back to normal lookup.

## Verification scope

The actual integration tests run real Core/Tether SQLite journals and parsers in
fresh processes against an independent SQLite HTTP fixture with fake authority.
They cover lost replies, exact retained-decision retries, concurrent resolution,
Core/Tether resolver revocation, changed bytes, and unchanged provider effect
counts. No live Slack, native session, production authority issuance, distributed
atomic revocation, power loss, or arbitrary supersession claim is made. Core #17
closure still requires final exact-source review, merge and criterion audit;
repository required-check policy is not bypassed by these local results.

## Exact execution receipt

The [final receipt](evidence/tether-resolution-20261117/qualification.json) binds
Core `86d5ea1` (runtime source `d1e39f2`) and Tether
`0393a700957bcb7dfe67754cebaf8fd98f3f05ac`, clean before and after execution.
All 134 top-level tests across all Rust feature targets passed in each run,
including all 41 outbox tests with six explicitly opted-in integration tests;
zero tests were ignored. The second run enabled Python optimization, not Rust
release optimization. The independent expected-binding test rejected six
mutations in both Python modes. Strict mypy, isolated Ruff and rustfmt also pass
for the changed inputs. The missing timestamp-length guard first produced the
retained failing regression; that diagnostic is not counted as final evidence.

This build used an owned disk-backed target, two compiler jobs, no debug symbols,
no incremental compilation and offline locked dependencies. It successfully
compiled and ran all features; this does not relabel the earlier PR #37 disk-full
attempt as passing. No fleet, network, security or production settings were
changed by this qualification. This branch is stacked on reviewed but unmerged
PR #37, whose required GitHub validation check remains a separate merge gate.
