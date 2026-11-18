# Durable Core Browser operation registry

Partial Core #3 / Browser #8 work. `browser_effect_registry::BrowserRegistry`
adds actual SQLite identity retention above the existing immutable wire client.
A separate [synthetic Temporal workflow](BROWSER_TEMPORAL.md) coordinates it;
actual Browser/site adapters remain separate.

## Identity, reservation and recovery

One retained effect is allowed per `(principal, intent, revision)`. Operation IDs
are additionally globally unique within this bounded registry. This deliberately
conservative rule prevents inventing a replacement operation ID to retry an
uncertain submission. A separately authorized new intent revision can have a new
operation; this API does not itself authorize that revision. Full browser/account/
origin/tab/native-session/frame/epoch target, original payload and actual digest
are immutable. Conflicting target, bytes or identity mappings fail before peer use.

Dispatch requires current Core authority, takes an immediate transaction, checks
retained identity and commits an unknown reservation **before** invoking the
existing Browser client. Only the call creating that reservation may dispatch.
Duplicate dispatch obtains fresh Core authority but returns Unknown without any
peer call, even when a prior terminal observation exists. It never automatically
requests reconciliation under a dispatch grant or returns cached success.

This at-most-once rule has an intentional liveness cost: a process dying after
reservation but before submission can leave an operation permanently unknown.
Missing observations do not restore a right to send. Recovery requires explicit
reconciliation of the same target, using retained payload/digest and separately
checked reconcile authority. An absent reservation cannot bootstrap reconciliation.
No DB transaction is held across a peer exchange.

Fresh peer observations are persisted under another immediate transaction with
current authority and retained binding rechecks. Unknown never erases a retained
terminal observation, nor promotes historical confirmation into a current result.
Contradictory confirmed/rejected observations set a sticky conflict flag and return
Unknown. Overlapping responses cannot silently overwrite each other as though
coherent. There is no conflict-resolution or replay permission implicit in this
version. `historical_snapshot` is explicitly local historical metadata, never a
current site receipt or authorization.

## Storage boundary

A dedicated file uses application ID `0x42524547` and schema version 1. Only an
empty unclaimed database may initialize. Missing/incompatible tables, triggers,
version or application marker are not repaired. Binding-update and deletion
triggers retain reservations. SQLite WAL/FULL, bounded busy waits and page count
provide local process/concurrency behavior, not storage-device power-loss proof.
The registry permits at most 1,024 rows, payloads up to 1 MiB and target JSON up to
8 KiB. Relevant reads validate exact target/key/operation/digest and current schema.
No background service, migration, backup, credentials or production activation is
introduced.

## Verification classes

- Rust unit tests use actual SQLite, independent peer call counts, conflicting
  connections, capacity, authority loss and corruption/schema negatives.
- Fresh Rust child processes exit inside peer calls before or after an independent
  fsynced site-effect file. Parent recovery must never dispatch again. Separate
  simultaneously released child processes prove one dispatch owner. These are
  real process-exit tests, not retained in-memory fake objects.
- Opt-in integration extends the pinned Browser receiver lane from
  [Browser wire client](BROWSER_EFFECT_WIRE.md): Core registry plus actual Browser
  GuardedJournal retain one independently counted SQL site effect after lost site
  and bridge responses. Fresh Browser authority is still required for readback;
  stored Core confirmation does not bypass it.
- Deliberate mutants test reservation ordering and semantic identity retention.
  Named behavioral failures, not compilation failures, are required.

Exact candidate/tree, commands and results are recorded in the PR receipt. These
are synthetic site effects and actual local persistence, not a live browser/site
or production issuer. A separate [synthetic Temporal lane](BROWSER_TEMPORAL.md) now coordinates this
registry. Actual site-specific observations/input, composition integration,
distributed fencing and full Core #3 / Browser #8 acceptance remain open.
