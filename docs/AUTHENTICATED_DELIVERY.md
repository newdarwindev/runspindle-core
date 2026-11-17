# Current artifact evidence at the delivery boundary

Bounded Core #4 follow-up, stacked on the explicit-resolution PR #38 rather than
merged main. It adds no signer, native provenance issuer, provider, credential,
service, workflow activation or delivery permission. Host #8 producer custody is
still a separate prerequisite. Existing legacy APIs and synthetic script CLI keep
their prior meaning; this opt-in lane does not silently upgrade those callers.

## API and checks

With `authenticated-artifacts` and `delivery-runtime`, `AuthenticatedOutbox`
privately owns an existing `Outbox`. Its `deliver`, `reconcile`, `resolve` and
`resolve_wire` methods require current artifact `Trust` and `Clock`, in addition to
separate exact `DeliveryAuthority` and, for resolution, `ResolutionAuthority`.
There is no raw inner-outbox accessor, public no-op artifact guard, serialized
capability or automatic production activation.

The existing authenticated evidence verifier is reused, not replaced. Every check
binds the exact Accepted/Validation/ArtifactRef, actual immutable BLOB length and
SHA-256, exact known evidence schema, canonical producer/validator signatures,
current role-bound keys, scope, epochs, audience and validity intervals. Missing
legacy-only evidence, missing/incompatible schema and unavailable clock deny.
Schema absence is not repaired by the verifier. Ordinary outbox opening still
uses its existing bounded schema initialization; that does not create artifact
attestations or establish their authority.

The crate-private helper accepts the caller's connection. Guards outside an
existing transaction take a read snapshot; guards inside reservation, settlement
or resolution use that exact existing transaction, never a nested BEGIN or a
second writer connection. Fresh artifact checks follow delivery/resolver callbacks
and precede every send/readback; reservation, settlement and resolution recheck
under lock, including cached Delivered and exact resolution replay. The first
`ResolutionBridge` decision-retention peer call is guarded too, without exposing
the raw outbox. The specialized bridge keeps its existing fresh independent
readback and append-only decision semantics.

Callbacks must be bounded and non-reentrant. These checks are not an atomic
transaction with an external issuer, clock or provider. An issuer revocation after
the final check can still race an external effect. No arbitrary cross-callback
mutation is claimed to establish a joint remote authority snapshot.

## Uncertain outcomes and quarantine

Post-effect denial does not undo the effect. The committed reservation and any
observed acknowledgement remain; a later authorized call can only reconcile, not
resend. Both explicit and inferred contradictory readback evidence are durably
quarantined even if authority changes during readback. This conservative retention
is not permission or success. Restored authority cannot silently clear the flag;
explicit separately authorized resolution remains required. Existing Tether
sticky-conflict and exact-generation resolution limitations remain unchanged.

## Verification and boundaries

Tests use real SQLite stores, actual signature verification, synthetic trust and
in-process transport/peer spies. They exercise both producer and validator
revocation after callbacks, absent clock/expiry, missing evidence/schema, separate
delivery and resolver denial, settlement-lock and exact-replay checks, and a
bounded fresh child process exiting after an observed send and denied completion.
Independent SQL rows/evidence and send/readback counts establish no-resend and
quarantine; the tests do not demonstrate live producer custody or Slack effects.
A deliberate guard-bypass mutant must fail a named behavioral assertion after
an unchanged baseline passes. Final source/log hashes and actual results are
recorded separately in the qualification receipt.

Reproduction uses existing offline dependencies and the configured feature set:

```
cargo test --offline --all-features --test authenticated_artifacts
cargo test --offline --all-features -- --include-ignored
cargo clippy --offline --all-features --all-targets -- -D warnings
cargo fmt --all -- --check
python3 tools/check_scaffold.py
```

The optional Core→Tether cross-process tests require the exact Tether source pin
in the receipt. This local lane is not the required GitHub isolated-runner check,
full composition qualification, OS-level network isolation, power-loss testing,
or completion of Core #4/#18. Existing signed-artifact mutation suite coverage
must not be inferred from the new guard-bypass mutation.
