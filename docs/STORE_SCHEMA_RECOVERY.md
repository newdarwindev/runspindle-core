# Fail-closed store reopening (unqualified source candidate)

Bounded Core issue #19 work, based on main
`263a00442f10096ae691be9b479f59a90e53d1ca`. This does not complete #19, its
admission-store scope, or parent #1. Rust compilation, behavioral tests and
mutation execution are not run in this workspace: the approved Docker runtime
boundary remains inaccessible. Source/static results are separate below.

## Invariant and change

Previously `Outbox::open` could recreate a missing `outbox` table even when
`outbox_schema` still identified an existing journal. Accepted bytes could then be
sent again under a newly created reservation. The candidate fails reopening
instead of treating missing durable identity as a first operation.

`Store::open` requires the existing Core-authored v1 artifact tables and retention
triggers under an immediate transaction. `Outbox::open` requires its version
marker, operation table and binding/retention triggers. Required object types,
column/constraint declarations and trigger bodies must match the known layout.
Runs of SQL whitespace outside quoted literals collapse to one separator; keyword
case outside literals is normalized. Token boundaries and quoted contents remain
intact. Equivalent layouts with different punctuation spacing are conservatively
rejected rather than inferred to be compatible. Unrelated
objects are not removed or prohibited. This is not a generic SQLite schema
compatibility or repair engine; changed layouts need an explicit migration.

The Core artifact `user_version` stays 1. The independent outbox version becomes
2 to record that the additive evidence schema committed. Valid v1 stores with no
evidence schema acquire it atomically; valid v1 stores already carrying complete
evidence retain it. Both preserve reservations, receipts, generations, conflict
flags, bindings and accepted bytes. A partial audit schema is rejected. v2 must
retain all required evidence objects; reopening never recreates them. A missing
version marker with surviving outbox objects is rejected. Unknown versions or a
missing marker row do not acquire a new identity.

DDL and the v1-to-v2 marker update share one immediate SQLite transaction. A
failed version update rolls back new evidence objects. Concurrent reopeners
serialize and reread the version under the lock. A successfully upgraded v2
store is intentionally not readable by older v1-only Outbox binaries. Rollback
means failed migration leaves v1 intact, not downgrading a committed v2 store.
No row deletion, pruning, backups, live repair, resend permission or operational
activation is added.

## Authored checks, not execution evidence

- A fresh process reopening a journal with its operation table removed is blocked.
  A separate peer SQLite ledger independently counts sends and lookups, including
  attempted duplicate sends rejected by the peer. Accepted bytes survive.
- Two fresh processes migrate the independently authored legacy v1 fixture while
  retaining the exact reservation. Reconciliation observes one existing effect
  rather than sending again.
- A synthetic trigger aborts migration after evidence DDL and before the version
  write. Independent SQL checks observe no new schema objects, unchanged v1 and
  retained operation identity; a fresh-process retry upgrades after removing the
  fault. This is transaction rollback, not a power-loss or SIGKILL claim.
- Valid SQLite `TEXT PRIMARYKEY` and `TEXTPRIMARYKEY` declared-type fixtures
  independently allow duplicate artifact identities and outbox operations and
  report no primary key through `pragma_table_info`. Reopening must reject these
  layouts without touching duplicate rows or reaching transport. A deliberate
  separator-removal mutation must fail the artifact regression.
- Missing/no-op retention triggers, missing tables/marker rows, unknown versions,
  changed table shapes, partial evidence and non-SQLite file bytes reject. Existing
  v1 evidence is preserved. Subprocess waits are bounded and killed/reaped on
  failure; temporary stores use RAII cleanup.
- The deliberate `missing-journal-recreated` mutation replaces the required
  journal-schema check with CREATE IF NOT EXISTS. The no-resend test must detect
  the second send attempt. Compilation failure never counts as mutation detection.

On the already-approved isolated runner, the bounded commands are:

```
cargo fmt --all -- --check
python3 tools/check_scaffold.py
python3 tools/offline_rust.py test --features artifact-store --test durable_artifacts
python3 tools/offline_rust.py test --features delivery-runtime --test outbox
python3 tools/offline_rust.py clippy --features delivery-runtime
python3 tools/check_outbox_mutations.py
python3 tools/check_artifact_mutations.py
python3 tools/validate.py
```

The offline Rust wrapper uses the existing architecture-specific builder IDs and
network-disabled container policy in `temporal/toolchain.json`. Do not run a native
fallback or acquire another runtime to evade an unavailable approved boundary.
Exact source-input digests and stage status are in `store-schema-recovery.json`.

## Supported assumptions and remaining gaps

The existing storage assumption is component-owned SQLite with local filesystem
locking, FULL synchronous WAL and intact journal/sidecar semantics. The candidate
adds no evidence qualifying any filesystem or power-loss behavior. Filesystems
that do not honor SQLite locking/sync requirements remain unsupported/unqualified.

The process is not protected from a concurrent hostile database writer. Complete
loss of every outbox object and its marker is indistinguishable from an
artifact-only store; v1 absence of the entire audit schema is indistinguishable
from the historical pre-audit layout. Neither condition establishes full recovery
or evidence completeness. Logical row corruption, admission/Temporal history
recovery, storage-device faults, upgrade compatibility beyond these layouts,
authenticated Tether transport and production/native/live acceptance remain
separate open work. No detected corruption is interpreted as permission to resend.
