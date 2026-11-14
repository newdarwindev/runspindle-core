# Offline verified authority v1

This optional `verified-authority` feature implements core #16 after composition
#43's `runspindle.envelope.v1` contract. Its scope is synthetic offline admission
and scripted host dispatch, with actual Ed25519 verification, Temporal and SQLite.
It does not authenticate the historical development workflows, host H02, delivery
H07, production callers, a live provider, native peer, browser or Tether transport.

## Trust and signed bytes

The worker receives an explicit local helper configuration file containing an
absolute SQLite path, issuer, actor, audience and allowed actions. Requests carry
only the opaque grant reference inside the admission envelope. They cannot select
helper paths, trust roots, actor identity or actions. Missing/unreadable helper
configuration starts a deny-only authenticated worker. It never falls back to a
script's authorized flag. The separate development worker preserves old behavior. Binaries compiled without
verified-authority reject authenticated-worker before opening a connection.

The helper opens the authoritative database read-only on every check, in a single
read transaction. It never creates/rebuilds a missing store and never caches
positive decisions. Missing schema/tables/rows, conflicting primary identities,
unknown keys, revoked grants/keys, unavailable reads and expired/future-dated
state deny. SQLite's signed integer range bounds stored timestamps; values that
cannot be decoded deny. Local operator state is trusted; malicious database
rollback, compromised helpers and clock integrity are outside this offline lane.

Assertions contain exactly version, issuer, key_id, actor, audience, grant_id,
request_digest, actions, policy_revision, target, not_before_ms and expires_at_ms.
Version is `runspindle.authority.v1`. Sign the bytes
`runspindle.authority.v1\0ed25519\0` followed by canonical JSON (sorted object keys,
compact ASCII strings, decimal unsigned integers). Verify with ring 0.17.14's
Ed25519 verifier. The request digest is lowercase SHA-256 of the exact canonical
UTF-8 versioned admission envelope. The authenticated entrypoint rejects
noncanonical request bytes. Grant references, message identities, policy and
entire task/revision/operation/host/workspace/lease target must match exactly.
The current admission contract has a host/workspace target; delivery account and
destination mappings are wire conformance only and grant no delivery authority.

Permitted actions are the closed enum admit, dispatch, reconcile, cancel, stored
once each in that order. Every signed action must also be configured locally.
There are no wildcards, inferred permissions or fallback versions. Validity is
half-open: not_before_ms <= now < expires_at_ms. Trust root and authoritative
state validity intervals must also contain now. A state validity interval is an
explicit operator freshness bound, not a claim about remote issuer availability.

The synthetic issuer creates an ephemeral key in a disposable mount with mode
0600 and a durable SQLite store with immutable grant rows and retained revocation
tombstones. The verifier holds no private key. Test keys, assertions and raw
histories are cleaned up and never committed. Histories retain canonical request
metadata and opaque grant references, never assertions, signatures or keys.

## Deterministic workflow and effects

`AuthenticatedRegistryV1` and `AuthenticatedOperationV1` use the dedicated
`runspindle-authenticated-v1` task queue. The registry calls the authority activity
before mutation, reuses RegistryState's conflict/deduplication/capacity logic in
one activation, and checks dispatch permission again before scheduling each
child. An admitted-but-expired pending task retains its original identity and a
failed pre-dispatch snapshot, with no child scheduled. Duplicate delivery retains
the original child ID; a new reference does not silently replace pending grants.

The operation entrypoint shares the historical deterministic operation loop. Its
activity selector uses AuthenticatedPorts, which checks the exact canonical wire,
call admission and action again inside the activity immediately before crossing
the adapter seam. An earlier recorded verifier result cannot authorize a fresh
launch after replay/restart. Only a newly scheduled launch is attempted once.
A lost response is unknown, and recovery cannot dispatch again. Cancellation
requires cancel permission at update and at the adapter call; reconciliation
requires reconcile permission. Denial after possible dispatch preserves unknown
outcomes and the original session identity. Cancellation acknowledgement remains
separate from an observed terminal outcome. Cancel permission does not imply
reconcile permission, or vice versa.

The verifier and effect are separate boundaries: this is a fresh activity check,
not an atomic transaction with an external provider. Revocation committed before
that check denies; no instantaneous revocation or exactly-once external-effect
claim is made. Development operation IDs remain globally unique within the single
registry/Temporal namespace. Multiple registry authorities remain unsupported.

Historical AdmissionRegistry, OperationWorkflow, HostAdmissionRegistry,
NativeOperationWorkflow and DeliveryWorkflow names/payloads remain registered for
replay. Existing script flags are still synthetic additional deny conditions.
No production adapter is selected by this feature.

## Intent, contracts and negative evidence

This implements desired-intent.md's scoped lifecycle without changing authority
through optimization or feedback. Envelope mismatches and stale trusted receiver
context reject before adapter invocation; signature, actor/audience, action,
target, policy, time and unavailable-state tests assert zero forbidden calls.
Real Temporal tests independently inspect child IDs, peer effects, cancellation,
SQLite revocations across worker/server restart, and replay without assertions or
fresh effects. Wire-sequence and dispatch-guard mutations must fail behavioral
assertions. Independent review is user-waived; no independent review occurred.

See docs/VERIFIED_AUTHORITY.md for reproducible commands and measured evidence.
