# Exact-destination scoped admission candidate

Related: Core #16 and parent #2. This is an opt-in **component-tested candidate**,
not full issue acceptance. Existing envelope v1, authority v1 and historical
workflow entrypoints retain their host/workspace-only meaning. No existing grant
acquires delivery permission.

## Closed wire and signing contract

`runspindle.scoped-admission.v1` is a canonical, unique-key JSON object with exactly:

- `version`: the literal above;
- `request`: an existing `runspindle.envelope.v1` admission-request envelope;
- `destination`: exactly `account_id`, `destination_id`, `delivery_operation_id`.

Each destination value uses the existing bounded identifier grammar. The delivery
operation must differ from the native operation. An adapter must map its exact
workspace/channel/thread to the destination identifier without lossy normalization;
this module does not claim a Slack routing adapter is integrated.

The existing assertion fields are retained, but `version` must be
`runspindle.authority.v2`. Its signature domain is
`runspindle.authority.v2\0ed25519\0`, followed by canonical assertion JSON. The
`request_digest` is SHA-256 of the **entire canonical scoped wrapper**, not just its
nested admission. Consequently account, destination, delivery operation, actor,
intent/policy revision, host/workspace/lease and grant identity cannot be changed
without invalidating the signature. Unknown versions/fields, duplicate keys,
noncanonical bytes, missing targets and non-admission payloads reject.

The ordered action vocabulary adds `deliver` after `cancel`. V1 verification
explicitly rejects this action, including a v1 assertion containing it. V2 grant
and trusted helper must both permit each action. No wildcard or implicit delivery
authority follows from admit/dispatch. The unchanged authoritative SQLite reader
performs a fresh read transaction and signature/time/revocation checks on every
invocation. Read-only state, missing trust source and stale state fail closed. The read transaction
ends before the adapter call. Revalidation is not an atomic revocation fence
covering a subsequent external effect; a concurrently revoked grant can race
after the final check. External systems must enforce their own fencing protocol.

## Entry and effect boundaries

The separately registered `ScopedRegistryV1` and `ScopedOperationV1` use queue
`runspindle-scoped-authenticated-v1`. The CLI `scoped-worker` and commands
`start_scoped`, `admit_scoped`, `scoped_registry`, `scoped_operation`, `cancel_scoped`
do not replace historical registrations. Replay registers both sets. A scoped
registry never accepts a v1 wire. It reuses bounded registry conflict/retention
logic; immutable grant references and signed complete requests prevent an alternate
destination from being treated as an authorized same-intent replay. Authority is
checked before child creation and again immediately inside the shared port adapter.
Only opaque wire/grant references, not signing keys or assertions, enter workflow
history. Production issuer provisioning is not implemented.

`ScopedDeliveryAuthority` implements the existing Outbox authority interface. It
requires an exact task/revision/operation/policy/host/workspace/lease match to the
accepted artifact, all three destination values, and fresh V2 `deliver` authority.
The real Outbox invokes it before reservation, immediately before sending, and
around readback. It neither accepts artifacts nor resolves quarantined receipts.
Artifact-validation authority and exact accepted-byte custody remain separate.
The API is available to an integration adapter; no new real Tether transport or
Temporal delivery workflow is claimed. Existing development delivery entrypoints
remain explicitly synthetic and are not promoted to this lane.

## Verification and remaining acceptance

Targeted checks use Rust 1.94.1, locked crate sources and Debian protoc
3.21.12-3+deb12u1, whose executable hash matches the repository pin. The native
Strix build is **not** the pinned prepared image and is not a substitute for the
full qualifier. Tests cover both versions' forged/expired signature matrix,
wrong account/destination/operation, no forbidden sends/lookups or journal rows,
actual SQLite outbox delivery/readback, revocation, and fresh-process trust reads.
A removed-account-binding mutant fails the independent outbox/transport assertion.
The default mutation runner includes that case, with original source restored.

Bounded targeted command after acquiring the already pinned dependencies:

```
timeout 300 cargo test --locked --offline --features verified-authority
```

Set `PROTOC`, `PROTOC_INCLUDE` and its library search path to an independently
verified extraction of the pinned packages if the compiler is not installed.
This command acquires nothing. Temporary synthetic stores are removed by fixture
owners; no real account, provider, persistent job or runner setting is changed.

Full qualification remains blocked: neither accessible non-Mac machine has the
exact x64 builder image `sha256:9045681d1e2443e55bffe0c86dd93ee262157ea807fbf55c4e58e82699c9a53e`.
The checked-in acquisition recipe identifies the official Rust base and Debian
packages, but no registry locator for that final prepared image. Official-source
search did not locate a published final image. Its pin was not replaced with a
locally built substitute. Fresh Temporal dispatch/revocation/restart/replay for
these new workflow registrations, independent producer/consumer scoped-wire
qualification and end-to-end delivery integration remain **not run**. Existing
v1 historical integration reports do not transfer. Core #16 remains open.
