# Core contract candidate: implementation evidence

Prepared 2026-11-13 for independent review. This is an original dependency-free
implementation proposal, not a port of legacy executable code or a production release.

## Exact base and scope

- Composition base: `d3a828be44294ec20fe951963e0bbc7482a988df`.
- Core base: `0bf9b5e4d0f88181d6f8156231c3578512cb84f3`.
- Changed implementation: `src/identity.rs`, `src/artifact.rs`, `src/lib.rs`.
- Synthetic fixtures: `tests/contracts.rs` plus the activation test in `src/lib.rs`.
- Proposed behavior and trust boundaries: `specs/contracts.md`.
- Metadata, README, specification index and structural checker reflect the
  partial contract candidate. The existing local-only Rust CI workflow is now
  configured for structural checking, formatting, offline synthetic tests and
  offline clippy. No toolchain installation, dependency or product gate was added.

`artifact.rs` models produced/validated/accepted/delivered with externally supplied
evidence tied to exact identity, policy and digest. Its single-target delivery
model preserves pending/uncertain outcomes and rejects conflicting replays.
`identity.rs` distinguishes transport duplication, known revisions, new revisions
and explicit new intents; it compares host/workspace pins without fallback.

## Executed local component checks

The final Rust sources and Cargo manifest match the `final_code_sha256` entries in
[local-contract-tests.json](local-contract-tests.json). An independent local
Linux x86_64 execution used Rust/Cargo 1.94.1, rustfmt 1.8.0-stable and Clippy
0.1.94. No dependencies or toolchains were installed.

- Initial formatting check failed. The disposable test copy was formatted with
  `cargo fmt --all`; only three Rust source/test files changed formatting.
- Final `cargo fmt --all -- --check`: passed.
- Final `cargo test --offline --all-targets`: **17 passed, 0 failed** (one
  activation test and 16 synthetic contract tests).
- Final `cargo clippy --offline --all-targets -- -D warnings`: passed.
- Structural check passed. The unchanged operational acceptance gate returned
  `blocked` and exit 78, an expected negative control, not integration acceptance.

The receipt retains initial and final source hashes, commands, exit codes and
counts. Its code inputs are byte-identical to this candidate. Later documentation,
metadata and workflow configuration are separately reviewed, not claimed to have
been executed as GitHub CI by the local test run.

Earlier [preparation-receipt.json](preparation-receipt.json) records the original
pre-format source snapshot's structural checks in an environment without Rust.
It is historical preparation evidence; the local test receipt above supplies the
actual Rust execution evidence.

The suite covers exact digest/task/revision/artifact/policy mismatches, stale
validation and acceptance references, illegal transitions, delivery before
acceptance, duplicate/conflicting delivery and receipt identities, uncertain
reconciliation, namespace isolation, payload/policy/host reuse conflicts, and
wrong/offline/unknown host observations.

`rust-ci.yml` is configured to run structure, fmt, offline tests and offline
clippy on a pre-provisioned isolated local runner. A missing toolchain fails the
job; no installation, skip or cloud fallback is present. **No GitHub CI execution,
qualified runner, integration, dev/production deploy or live acceptance is proved
by these local component results.**

## Remaining gaps and promotion gates

1. Preserve independent review and exact local test evidence when this slice
   changes. Qualify local runner isolation and verify actual GitHub CI outcomes.
2. Agreement on the proposed types, authoritative ID assignment, revision
   supersession and destination authorization. No protocol compatibility is claimed.
3. Trusted content hashing, validation, actor/policy/approval evidence and provider
   receipt verification. Typed values do not manufacture any of these guarantees.
4. Durable uniqueness, atomic outbox persistence and writer fencing; concurrency;
   restart recovery; uncertain effect readback and explicit no-effect reconciliation
   before safe retry. `Artifact` is cloneable: separate clones/objects can each
   return `Started`, so the reservation is per object, never globally exactly-once.
5. Native session/lease binding, host discovery and offline queue, Temporal,
   browser/credential integration and the other component adapters. `HostObservation`
   has no timestamp, freshness limit or version evidence; an old online observation
   can still compare as eligible. Freshness and fencing remain unresolved.
6. Bounded ledger retention, replay expiry and scalable admission: `AdmissionLedger`
   retains all admitted events/revisions, and known-task detection scans keys
   linearly. No production resource or throughput guarantee is supported.
7. Isolated integration, package/distribution, dev and production rollout,
   health/readback and rollback evidence on authorized targets.

`activate()` still returns `ReadinessError::NotImplemented`. The operational
acceptance gate is unchanged and exits 78. This candidate does not unblock product
acceptance, production activation, old-repository archival or credential use.
