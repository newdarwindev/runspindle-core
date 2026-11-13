# Proposed core contract model

Status: local implementation candidate. These model types are **not** an agreed
public cross-component schema, wire format or compatibility promise. There are no
third-party dependencies and no network, file, provider or native runtime actions.

This slice narrows the composition's `specs/system.md` requirements. It does not
replace the larger execution, identity, authority or restart requirements.

## Trust and evidence boundary

- `Sha256Digest` holds exactly 32 caller-supplied bytes. It does not hash an
  artifact, retrieve a URL, compare file bytes or prove the value is a real hash.
- Validation and acceptance are externally supplied evidence references. This
  library stores and compares their bindings; it does not perform validation,
  authenticate an actor, verify evidence content or grant approval.
- The artifact reference binds explicit task ID, request revision, artifact ID,
  policy revision and exact digest. Acceptance must also name the exact stored
  validation evidence ID. A previous artifact/revision/policy/digest or validation
  reference cannot accept a different artifact.
- Host observations and delivery receipts also require trustworthy adapters.
  A synthetic observation in a test is not evidence of a real host or effect.
- IDs are bounded opaque ASCII references, not URLs or raw user content. Syntax
  restrictions do not guarantee they contain no sensitive data. Callers must keep
  secrets and private content out of every ID and evidence reference.

## Artifact and delivery states

An artifact starts `Produced` with an immutable reference. `record_validation`
requires matching validation evidence and advances only `Produced -> Validated`.
`record_acceptance` requires matching acceptance evidence and its validation ID,
and advances only `Validated -> Accepted`. Repeating either transition or skipping
states rejects without changing the record.

Delivery has a separate state, initially `NotStarted`. `begin_delivery` requires
`Accepted`, binds one delivery ID and destination to the artifact, and records
`Pending`. This leaves the artifact `Accepted`: a reservation is not delivery.
Only a matching observed receipt advances the artifact to `Delivered`.

- Repeating the same pending request yields `AlreadyPending`, not `Started`.
- A lost acknowledgement may change `Pending -> Uncertain`. An identical retry
  yields `ReconcileRequired` and remains uncertain; there is no blind-retry reset.
- A matching observed receipt can resolve `Pending` or `Uncertain` to `Delivered`.
- Replaying an identical delivered request yields `AlreadyDelivered`; replaying
  its identical receipt is idempotent. A different external receipt is a conflict.
- Reusing the record with a different delivery ID or destination rejects. This
  deliberately models only one delivery target; multi-target delivery is absent.
- All mismatches and illegal transitions preserve the previous record.

`Started` describes a process-local reservation, never permission to contact a
destination. Acceptance here does not authorize sharing with an arbitrary target.
`Artifact` is `Clone`, and independently constructed records can share the same
reference. Two such objects can each return `Started`. Reservation and duplicate
protection are therefore strictly **per object**, not global, cross-clone,
cross-process or safe across restart/concurrent writers. An eventual adapter must
separately verify destination-specific authorization, enforce durable uniqueness
and writer fencing in an atomic outbox, and reconcile provider state before effects.

## Request delivery, revision and semantic intent

`RequestDeliveryKey` is an upstream namespace plus event ID. `IntentKey` is an
explicit semantic task ID plus request revision. `IntentSpec` additionally binds
the normalized semantic digest, policy revision and host/workspace pin.

| Input | Result |
| --- | --- |
| Unseen event and unseen task ID | `NewIntent` |
| Unseen event and unseen revision of a known task | `NewRevision` |
| Unseen event, known revision, identical specification | `ExistingRevision` |
| Known event, identical specification | `DuplicateDelivery` |
| Known event with any changed specification | `DeliveryConflict` |
| Unseen event, known revision with changed digest, policy or pin | `IntentConflict` |

Rejected registrations consume neither event IDs nor intent revisions. A matching
digest does not silently merge two explicitly different task IDs. A new revision
is not interpreted as a new semantic intent. This library does not normalize text,
create task IDs, enforce monotonic revisions, supersede work, schedule executions
or infer whether a revision needs new effects. Adapters must resolve those rules.

`AdmissionLedger` retains every admitted event and revision without a size limit
or expiry policy. Checking whether a task ID is known scans the intent keys
linearly. Resource bounds, replay-retention/expiry semantics and scalability are
unresolved; this is not a production ingestion service.

## Host pin

`check_host` compares the exact host and workspace first. A mismatch is an error
even if the offered host is offline. The same pinned host observed offline yields
`QueueForPinnedHost`, never eligibility or selection of another host. Unknown
availability is an error. A matching online observation yields only `Eligible`.
No queue or session is created, and no actual availability is probed.
`HostObservation` carries no observation timestamp, freshness limit or version
evidence. An old online observation can still compare as `Eligible`. Trusted
runtime observation, freshness validation and fencing must precede real execution.

## Explicit non-goals

No durable store, atomic outbox, concurrency across processes, recovery codec,
restart reconciliation, canonical serialization, hashing, validator, permission
engine, provider integration, native session identity check, lease-epoch fencing,
Temporal integration or execution exists. Cloned/reconstructed records or a fresh
ledger do not provide cross-instance deduplication. Runtime activation and product
acceptance gates continue to fail closed. None of the synthetic tests demonstrates
real validation, authorization, delivery, durable exactly-once effects or deployment.
