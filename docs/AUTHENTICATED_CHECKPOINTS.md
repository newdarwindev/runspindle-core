# Core F02: bounded interruption and upgrade acceptance audit

Related: [Core #2](https://github.com/newdarwindev/runspindle-core/issues/2),
[PR #33](https://github.com/newdarwindev/runspindle-core/pull/33).
This is mock-peer/real-component qualification, not production readiness or parent
closure. The composition pin update remains a separate prerequisite.

## Exact-source evidence

The full corrected aggregate passed at
`077963fa00ca587e7044c950a9b091d5ce90f541` with runtime image
`sha256:d4fb199e01ac69e53900ebf60f510043197ff0d43e9deed9ecf867f36b25d835`.
The subsequent publication changes evidence/status documentation only.
[Machine-readable receipts](authenticated-checkpoints-qualification.json) preserve
source fingerprints, source/binary/config/fixture pins, all phase log hashes,
checkpoint outcomes, old/new history digests and prior failed aggregate details.
The bounded reproduction command is in
[the checkpoint specification](../specs/admission-checkpoints-v1.md).

| Verification | Result |
| --- | --- |
| Authenticated/scoped registry boundaries | Four points × healthy/revoked × two modes × two repetitions: 32 passed |
| Semantic admission/dispatch/reply boundaries | Three points × healthy/revoked × two repetitions: 12 passed |
| Actual old-executable upgrades | Authenticated, scoped and semantic lanes, twice each: 6 passed |
| Explicit failure/SIGTERM cleanup | Both passed, including successful daemon listings and absent owned resources/temp state |
| Complete bridge regression aggregate | Passed, including repeated real Temporal/SQLite plus actual Tether journal recovery |
| Core Python / verified-authority Rust | 108 / 100 passed |
| Composition Python | 357 discovered, 3 existing skips |
| Tether normal / optimized | 425 discovered each, 1 existing renderer skip each |
| Formatting / strict Clippy / static debt gate | Passed; 407 existing static diagnostics, 0 new, no baseline change |
| Deliberate mutations | All five killed by behavioral assertions, source restored |

There are 53 top-level checks, one of which is the nested bridge regression
aggregate. The renderer skip is not visual acceptance. The isolated runtime
artifact boundary is not the separate fleet-runner qualification or live transport.

## Core #2 criterion mapping

| Criterion | Bounded evidence and remaining boundary |
| --- | --- |
| Duplicate returns the same task; conflicting replay denied; revisions explicit | Existing registry and semantic wire tests plus real concurrent authority integrations retain exact job/source identities. Checkpoint retries recover the same original job; child history permits no second or alternate child. Semantic batch imports only missing metadata, not stale execution. |
| Crash around commit/reply recovers without a second external effect | Generated update, pre-child, server-resolved child and lost reply boundaries are independently distinguished. Real SQLite server/worker restart produces one original-host launch in healthy cases and zero launches after downtime revocation. Actual prior executables generate histories before the new worker recovers/replays. |
| Actor string/approval flag cannot grant authority | Real Ed25519/current SQLite authority checks and forged actor/signature/scope/time/revocation cases remain in the aggregate. Synthetic issuer credentials are fixture inputs, never a production grant. |
| No alternate-host dispatch | Full immutable binding is checked against every independent peer-ledger entry; exact original child identity and scope are retained after restart. No fallback host is configured. |
| Positive/negative tests and deliberate mutation | Core unit/runtime assertions, independent ledger/history observations, four new interceptor regression tests, and five existing behavioral mutations all pass. The worker checkpoint only selects exact workflow type/ID and rejects replay/unknown activations. |
| Fresh-process actual persistence | Disposable real Temporal SQLite and authoritative grant SQLite are restarted in fresh processes. Tether journal recovery uses its actual SQLite implementation. Native peers remain scripted; power loss is not simulated or claimed. |

## Commit, scheduling and unresolved state

A generated workflow command is not a server commit. Admission mutation and a
subsequent child command may share a workflow-task transaction; the harness does
not invent a separately durable gap. Server history, not a local receipt alone,
proves child-start counts and absence of durable Launch scheduling at its specific
precommit checkpoint. An admitted client reply does not imply a child already
exists. Reports retain the actual pre/post observations.

After a pre-dispatch semantic interruption, revocation can reject the regenerated
Launch at the helper boundary while the workflow conservatively retains Unknown.
Both repetitions assert Unknown, no native session ID, no terminal revision, one
retained intent and zero launches. Replay succeeds with no added ledger effects.
This is an unresolved revoked operation, not completed execution or proof of
terminal cancellation. No atomic external effect/revocation fence is claimed.

## Specified old versions only

- Authenticated/scoped histories originate from published Core
  `4093613f026341682bf717c20aeb1d120eff70f2`, actual runtime image `57c1c93…`.
- Semantic histories originate from published Core
  `7f4e3bbd53fa8534a5827fd2272c245abb10f327`, actual runtime image `38986012…`.

Full image, executable and source digests are pinned and verified before running.
Old binaries actually execute before restart; this is not merely replaying new
histories with the same binary. Only these specified upgrades are qualified,
not arbitrary future workflow changes.

## Failed receipt preserved

The first aggregate passed all checkpoint/upgrade scenarios but rejected the
cancellation probe's diagnostic: the pinned platform Lane installs its own
SIGTERM handler. Its legitimate `lane cancelled by signal 15` message differed
from the outer CLI message expected by the wrapper. Exact owned resources were
independently confirmed absent. Only that expected diagnostic changed, and the
entire aggregate reran at the corrected source and passed. The earlier failed
receipt and log hashes are retained, not relabeled as a pass.

## Remaining integration prerequisites

Composition main `ba5a102…` still pins older Core `83296c3…`, Tether `c3fbfb9…`
and Platform `4e9a2b3…`. These tests use explicit qualified checkouts; they do not
silently upgrade those composition gitlinks. A separate bounded pin/evidence PR
must qualify the merged Core candidate, exact Tether #39 source and selected
Platform source without updating unrelated component pins. Tether #40's optional
raw-edit store is not covered by this evidence and Tether #8 remains partial.

Production issuer/key custody, controlled service ownership, actual native and
provider adapters, raw provider delete attribution, and live acceptance remain
outside this controlled qualification. No host security/access changes, persistent
services, live providers, MacBook or Codex tasks were used.
