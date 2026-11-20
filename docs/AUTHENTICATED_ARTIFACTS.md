# Authenticated artifact evidence (original implementation record)

Later bounded verification and the optional current-artifact delivery composition
are documented in [Authenticated delivery](AUTHENTICATED_DELIVERY.md). The
source-only statements below describe the original change, not that later lane.

Partial Core [#18](https://github.com/newdarwindev/runspindle-core/issues/18) and
parent [#4](https://github.com/newdarwindev/runspindle-core/issues/4), based on
`263a00442f10096ae691be9b479f59a90e53d1ca`. The original trusted `Validator` callback
was never an authenticated remote validator: it supplies a revision and boolean,
with no native producer principal or signed provenance. This opt-in component adds
that missing evidence boundary without relabeling historical acceptance.

Status: source and authored tests only. No Rust compilation, formatter, behavioral
tests, Clippy, mutations or aggregate runtime checks have executed for this change.
The approved Core Docker lane remains unavailable; no native fallback, replacement
runtime, installation or denied-Docker retry was attempted. Source/static checks
and exact input hashes are recorded in `authenticated-artifacts-evidence.json`.

## Closed attestations and trusted inputs

Feature `authenticated-artifacts` uses the existing pinned `ring` 0.17.14,
SQLite, serde and SHA-256 dependencies. No lockfile or existing envelope changes.
The evidence version is `runspindle.artifact-evidence.v1`. Two required attestations
carry producer and validator statements plus 128-character lowercase-hex Ed25519
signatures. Every nested structure rejects unknown/duplicate fields. Decode
requires exact sorted compact JSON with no trailing newline or encoding aliases.
Every object is sorted explicitly by UTF-8 key bytes during recursive encoding;
this does not rely on serde_json's default Map implementation or Cargo feature
unification. Strings retain JSON escaping, arrays retain order and the closed
schemas use only integer counters and booleans, not floating-point numbers.
The evidence is at most 65,536 bytes. There is no generic JSON override or URL/path
lookup. Existing binary artifacts may be empty and are bounded at 1 MiB; these are
artifact bytes, not the separate UTF-8 native-turn input contract.

Both statements bind the same complete scope:
- Existing exact ArtifactRef, including task/revision/operation/policy/host/
  workspace/lease, artifact identity/revision and lowercase SHA-256.
- Exact byte length, producer principal and native thread/turn/item IDs.
- Validator principal and revision, plus a positive authority epoch.

Each statement separately binds version, issuer, key ID, key epoch, audience and
half-open validity interval. The validator additionally signs the SHA-256 of the
entire canonical producer attestation, including its signature, and `passed=true`.
A producer proof cannot be replaced while keeping the validator proof. Signing
uses these distinct prefixes followed by canonical statement JSON:

- `runspindle.artifact-evidence.v1\0producer\0ed25519\0`
- `runspindle.artifact-evidence.v1\0validator\0ed25519\0`

The trusted callback returns an exact authorized Scope, audience, authority-state
lifetime and separate role-bound public keys. Each key binds role, issuer, key ID,
key epoch, principal, lifetime and revocation state. Missing state, wrong roles,
epochs, audience, principals, scope, signatures or validity intervals deny. There
are no wildcard grants, inferred aliases, default trust roots or positive caches.
`Deny` returns no authority. Trusted key/state types cannot be deserialized from
the untrusted attestation API. The callback must return current authoritative
state for every invocation; revocation is represented by unavailable state,
changed exact scope/epoch or a revoked key. Key reuse across roles is not implicitly
allowed: the caller must independently configure each role, and domain separation
still applies.

The trusted injected clock must return unavailable on detected rollback/failure.
Time is checked after trust resolution and again after cryptography, including
monotonicity within that check. No clock integrity or cross-call rollback defense
is claimed. Trust/clock callbacks must be bounded and non-reentrant. A fresh check
is not atomic with a remote issuer; revocation after the final check cannot be
claimed as synchronized with an unrelated authority system.

## Atomic acceptance and retained references

`AuthenticatedArtifacts` privately wraps the existing Store; it never replaces
its BLOBs, reference format or trusted-callback APIs. Artifacts are staged using
the existing Store. This path derives the legacy Validation from authenticated
scope, but independently verifies producer and validator signatures rather than
trusting a revision string or existing validation record.

Acceptance checks authority before the transaction and immediately after taking
the SQLite lock, before reading protected bytes. It then rehashes the actual BLOB
and checks exact length, rechecks fresh authority after the read and again before
commit. An exact prior validation can be reused only after this independent signed proof succeeds;
a conflicting prior validation denies. Validation, original acceptance and exact
canonical evidence commit in one transaction. Failed evidence writes, capacity or
last-moment denial roll back all new rows. No generator/input path is reopened.

The additive immutable `authenticated_artifact_evidence` table is keyed by the
existing task/revision acceptance identity. It retains the exact original
Accepted record, evidence bytes and evidence digest. Its table is bounded to
1,024 entries. Update/delete triggers retain records. A separate schema marker
permits only the known v1 layout; partial/missing marked layouts are not repaired.
Existing acceptances without evidence cannot be upgraded, even if their validator
revision matches. Exact duplicate requests reconcile; different evidence for an
accepted task/revision conflicts. Duplicate acceptance independently requires its
retained validation record to exist and exactly match; missing/corrupt records
deny without repair. New expiry, epoch, key or scope does not silently
rewrite an old acceptance. A different authorized intent revision is separate.

On restart, `read` reconstructs evidence from the database, verifies its digest,
canonical structure and exact accepted/validation records, reloads current trust,
verifies both signatures/lifetimes, rehashes actual bytes and checks authority
again before returning them. A historical acceptance remains stored when its
current authority is revoked or expired, but authenticated read returns an error.
The result type cannot be deserialized into an authority capability. Its plain
Accepted accessor is still only a reference and grants no delivery permission.

This branch is independent of held Core #26. It changes no Store::open/outbox
schema logic, existing schema versions or envelope fields; only three existing
identity-key helpers become crate-visible. The new auxiliary tables coexist with
#26's known-object guards, which ignore unrelated objects. Exact combined-tree
qualification is still required after integration. The concurrent formatting-only
Core #27 candidate is not incorporated or rebased into this branch; any later
integration must preserve its formatting changes and requalify the exact tree.
No source result substitutes
for #26's durability qualification or #18's real native provenance integration.

## Acceptance after a schema change

Core [#18](https://github.com/newdarwindev/runspindle-core/issues/18) and
[#19](https://github.com/newdarwindev/runspindle-core/issues/19). Open, accept and
read use the same authenticated evidence schema check. Acceptance checks the
version and recorded DDL after acquiring its SQLite write lock and before
reading the artifact bytes. A changed version or missing evidence trigger
blocks both a new acceptance and a duplicate through an already-open handle.

The regression uses synthetic signatures and two SQLite connections. One
connection changes the version or removes the retention trigger after the store
has opened. The existing handle and reopening both deny while the schema is
damaged. An independent SQL reader verifies the retained row counts. After the
fixture restores the schema, the signed evidence digest and bytes read back
unchanged, and acceptance of a new revision succeeds. With the acceptance
schema check removed, both requests succeeded under the unsupported version.

Local macOS arm64 verification at base
`374e0184885059d4e5de4114dc0fa95d0a24f7e6` used Rust 1.94.1 and protoc
3.21.12. The three suites passed 89 cases, with six optional Tether peer
cases skipped. Clippy, formatting, scaffold and 145 Python tests passed,
with three Python skips. The cases use synthetic identities and SQLite
logical changes. Power-loss, production custody and Linux qualification
of this change are outside those results.

```sh
python3 tools/offline_rust.py test --features verified-authority,authenticated-artifacts --test authenticated_artifacts --test durable_artifacts --test outbox
```

SHA-256 of the lockfile and focused inputs

```text
216df163e47e5f8756267a2b62a162bcc3897f6a54aee0ddfc2eb971fcac70c6  Cargo.lock
c99ac1a5b93ae1fa03e7e8bb6b637653a32116d49ad54716ad413133d4ed3b34  src/authenticated_artifacts.rs
2c15cab12835367649c6304f5e1e9f22a87c0dcad2a9e26899e968e5bebc1b68  tests/authenticated_artifacts.rs
```

## Authored regressions and execution boundary

Tests cover forged same-revision validators/producers, cross-role signature
domains, native identity/policy/principal/epoch mismatches, key roles/revocation/
lifetimes, exact bytes and lengths, legacy-acceptance rejection, immutable evidence,
failed persistence and final-check rollback, conflicting writers, fresh-process
reopen/reverification, malformed evidence, and no inherited delivery authority.
Regressions additionally cover missing/corrupt retained validations, valid changed
evidence accepted in an independent clean store but rejected as a replacement,
expiry/clock rollback within verification, and trust/clock denial after protected
byte reads in both acceptance and retained-read paths. Direct SQL rows, bytes and
callback counts are the authored observations.

`fixtures/authenticated-artifact-v1.json` is an independent literal JSON, Ed25519
and SHA-256 vector generated with Python stdlib and cryptography 50.0.0, without
Core imports. It retains exact canonical/signing bytes, public keys, signatures
and digests. The Rust vector test has not executed. The existing offline lockfile
is unchanged; a separate preserve_order feature-build is not claimed or run.

Seven mutations remove producer/validator verification, exact scope, final
authority, byte-length or retained-validation checks, or reverse canonical key
order. The harness first requires the unmodified full test binary and every
mutation-targeted named test to pass in the same isolated image, offline feature
set and private target. Only then may named behavioral failures count as mutation
detection. Compilation/baseline failures never qualify. All seven remain unrun.

Race stores are opened before workers spawn; bounded start-channel receives
replace an unbounded barrier. Child process waits are bounded and killed/reaped
on error; fixture directories are removed by RAII. Each mutation container has a
unique name. A `finally` block force-removes that owned container and verifies
its absence, including after a subprocess timeout. Each cleanup command has a
15-second limit; unavailable cleanup fails the harness. This is not a guarantee
against a killed runner or unreachable Docker daemon. No broad pruning occurs.

The existing Rust test/Clippy commands now enable `authenticated-artifacts`
alongside `verified-authority`. The new check is assigned exactly once to the
existing `mutation_2` partition. The workflow currently allows 75 minutes for
the existing 600-second artifact check, the authenticated artifact check and
setup. The authenticated check now has nine mutation cases and repeats the
clean and test commands for each. Their configured subprocess ceilings can
exceed the existing 3,600-second check budget.

For a private runner with a shorter VM watchdog, the harness accepts
`--shard 1` through `--shard 5`. Every shard runs the full clean baseline
before running up to two mutation cases in its private target. The subprocess
ceilings total up to 1,260 seconds per shard, plus target setup and other
overhead. The CI controller must measure that overhead and verify exact job
provenance and successful conclusions before joining the five receipts with
`--verify-shards`. The join checks the exact clean checkout revision, the full
baseline in every shard, and every mutation case exactly once. The current
GitHub workflow still calls the unsharded entry point and still requires its
original aggregate.

Use only the already-approved prepared isolated runner:

```
cargo fmt --all -- --check
python3 tools/check_scaffold.py
python3 tools/offline_rust.py test --features verified-authority,authenticated-artifacts --test authenticated_artifacts
python3 tools/offline_rust.py test --features verified-authority,authenticated-artifacts
python3 tools/offline_rust.py clippy --features verified-authority,authenticated-artifacts
python3 tools/check_authenticated_artifact_mutations.py
python3 tools/validate.py
```

No feature is enabled in a production worker or CLI. No signer, trust database,
credential, issuer service or native transport is installed. Test-only fixed keys
are public synthetic fixtures, not production credentials. A trusted producer's
signed native-identity claim is not independently proven to originate from an
installed native binary. Host-issued proof/custody, real validator execution,
external trust source, cross-component wire acceptance, deployment and live
qualification remain blocked/not run. Admission grants and envelope v1 do not
grant artifact or delivery authority. Existing legacy APIs remain trusted-code
interfaces and must not be substituted for this authenticated path.
