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

The earlier component-only report in `docs/scoped-authority-evidence.json` is
historical. The current bounded offline qualification is recorded in
`docs/scoped-authority-qualification.json`, at the exact source revision named
there. All 13 phases passed: scaffold checks, Core and composition Python tests,
Rust formatting and verified-authority tests, strict Clippy, three deliberate
behavioral mutations, Python/Rust v1 wire conformance, and two runs each of the
legacy and new scoped Temporal integration. The composition suite has three
explicit skips; those are not acceptance evidence. Static quality has zero new
diagnostics without baseline changes.

Both scoped runs use actual Temporal/SQLite and fresh worker/server processes.
They check conflicting revisions, replay, signature and destination tampering,
unavailable trust, expiry and revocation at dispatch, separate cancellation
authority, independent child histories and an fsynced scripted effect ledger.
Each observes four child IDs, three authorized launches, one cancellation, and
successful container/network/volume cleanup. No unauthorized launch is observed.
The issuer and host remain synthetic. Real Tether, providers and production
identity provisioning are not exercised. Revocation checks are not an atomic
transaction with external effects. Independent scoped producer interoperability
beyond the synthetic Python harness is not claimed.

### Reproduction and builder provenance

The default legacy builder pin is unchanged. Docker archive compatibility is
explicit opt-in, not a fallback. The selector hashes and parses the same opened
archive descriptor, requires the exact archive bytes/config/architecture, and
selects only the fixed config image already recorded in the Platform closure.
It never trusts an arbitrary archive tag. See `temporal/toolchain.json` and
`tools/prepared_builder.py`. The qualifier uses the exact prepared runtime in
`docs/runtime-build.json`. All sources, configs and manifests are checked for
changes before and after qualification.

With those exact dependencies prepared and pinned quality tools on PATH:

```sh
RUNSPINDLE_BUILDER_ARCHIVE=/absolute/path/to/verified-builder-closure.tar.gz \
python3 tools/qualify_verified_authority.py \
  --platform /path/to/pinned-platform \
  --platform-prepared /path/to/prepared-temporal \
  --composition /path/to/pinned-composition \
  --prepared local/runtime-prepared --report-dir /path/to/report
```

The client fixture runs as its owning UID only when its private effects directory
is mounted. All existing Lane resource limits, dropped capabilities,
no-new-privileges, read-only filesystem and internal-only network remain intact.
No fleet controller or host security configuration is changed.

Earlier attempts failed because mocked builder tests inherited the explicit
archive environment and composition submodules were uninitialized. The tests now
isolate that environment; exact pinned submodules were initialized without pin
changes. The subsequent full qualifier passed. Final independent candidate
review remains required; this document does not close broader parent issue #2 or
claim live acceptance.
