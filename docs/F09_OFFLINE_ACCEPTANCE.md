# F09: exact artifact acceptance, bounded offline acceptance

Criterion audit for [Core #4](https://github.com/newdarwindev/runspindle-core/issues/4).
This is actual SQLite storage, cryptographic verification and process-recovery
verification with invented identities and keys, not production issuer custody.

## Original criteria and implementation

| Requirement | Implementation and verification |
| --- | --- |
| Immutable identity and actual-byte hashing | [Store](../src/durable.rs) stores bounded BLOBs and computes SHA-256 itself. `actual_bytes_are_hashed_and_changed_identity_or_bytes_conflict` in [durable tests](../tests/durable_artifacts.rs) checks an independent literal digest, changed bytes, revisions and invalid identities. |
| Stale validation and caller-declared approval fail | Persisted validation binds exact artifact, validator revision and policy. `validation_cannot_be_skipped_forged_overwritten_or_inherited` rejects skipped/forged validation, denied authority and changed revision; authenticated tests reject forged producer/validator signatures even with unchanged revision. |
| Mutable URL alone never proves integrity | The API accepts bounded actual bytes and exact closed references, not a URL resolver. `byte_replacement_length_mismatch_and_failed_validation_never_accept` rejects changed persisted bytes, wrong lengths and failed validation. |
| Unauthorized acceptance fails | [AuthenticatedArtifacts](../src/authenticated_artifacts.rs) checks role-bound Ed25519 proofs, exact authorized scope, epochs, expiry and revocation before protected reads and commit. `roles_epochs_revocation_audience_and_lifetimes_fail_closed` and `signed_native_identity_policy_principal_and_epoch_need_exact_authority` exercise negative authority cases. |
| Atomic acceptance under conflicts and crashes | SQLite immediate transactions commit validation, acceptance and canonical evidence together. Both durable and authenticated conflicting-writer tests permit exactly one coherent acceptance. `fresh_process_precommit_exit_recovers_without_partial_acceptance` hard-exits at the final precommit authority callback without Rust unwinding, then a separate recovery process finds zero partial rows, original bytes and a successful authorized retry; a conflicting retry cannot mix evidence. |
| Positive/negative tests and forbidden effects | [Authenticated tests](../tests/authenticated_artifacts.rs) inspect independent SQL row counts and retained wire evidence. Failed acceptance cannot create acceptance/evidence rows; downstream authenticated delivery negatives assert zero transport calls. Existing byte/validation regressions fail closed before delivery. |
| Deliberate invariant mutations | [Artifact mutants](../tools/check_artifact_mutations.py) remove digest, validation, retained-validation and schema-token guards. [Authenticated mutants](../tools/check_authenticated_artifact_mutations.py) remove signature/scope/authority/length/retained-record checks, corrupt canonical order and deliberately commit before final authorization. Behavioral test failures are required; authenticated mutants and the retained-validation mutant require named failures. Compile failure is not success. |
| Fresh-process durability | The new precommit exit regression contrasts with `fresh_process_reopens_exact_evidence_and_rechecks_revocation` (postcommit exit). `corrupt_retained_validation_blocks_fresh_process_byte_release` independently corrupts retained validation/link fields and requires no accepted-byte release after reopening. |
| Versioned envelopes, transitions, errors and adapters | [Contracts](../specs/contracts.md), [authenticated evidence](AUTHENTICATED_ARTIFACTS.md), [authenticated delivery](AUTHENTICATED_DELIVERY.md) and [schema recovery](STORE_SCHEMA_RECOVERY.md) specify closed versioned shapes, invalid combinations, produced/validated/accepted states, current trust/clock callbacks, immutable rows and fail-closed recovery. |
| Bounded storage and injected dependencies | Actual Store limits artifacts to 1 MiB and tables to 1,024 rows. Authenticated evidence is bounded to 65,536 bytes. IDs, trust, clock and validator callbacks are supplied explicitly; tests inject final authorization denial, SQL failures and process interruption. No hidden network adapter or implicit credential lookup. |

## Evidence scope and dependencies

The original trusted callback remains a local adapter, not an authenticated remote
issuer. The optional authenticated path is separately exercised with real Ed25519
verification and synthetic fixed test keys. Neither path converts a caller's boolean
approval or URL into production authority. The historical source-only notice in
AUTHENTICATED_ARTIFACTS.md records its original state; later PR receipts record
actual pinned-builder execution.

Core #5 and #17 retain the separately verified accepted-byte delivery and pinned
Tether wire slice. Shared fixture acceptance belongs to composition #1. The current
Temporal checkpoint matrix is additional integration evidence, not a substitute
for the direct F09 storage criteria here.

Native producer/key custody remains Host #8/Core #18. Storage-device/filesystem
power loss remains Core #19. Live providers, arbitrary upgrades and full-system
hero acceptance are not claimed by closing this bounded offline functional issue.
Hard process exit is deliberately not labeled power-loss testing.

## Reproduction

Use the repository's pinned prepared builder and locked offline dependencies:

```sh
python3 tools/offline_rust.py test --features verified-authority,authenticated-artifacts
python3 tools/check_artifact_mutations.py
python3 tools/check_authenticated_artifact_mutations.py
python3 tools/offline_rust.py clippy --features verified-authority,authenticated-artifacts
python3 tools/check_scaffold.py
```

The default full Rust suite leaves the six explicit Tether opt-in tests ignored;
those belong to the separate pinned cross-component lane documented in F10.
Exact final source/tree, command results and log hashes belong to the PR receipt.
