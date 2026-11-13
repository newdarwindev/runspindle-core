# Pinned core synthetic fixture consumer

Partial progress toward [core #1](https://github.com/newdarwindev/runspindle-core/issues/1).
This adopts the test-only admission/native/artifact subset from the reviewed
composition fixture kit. It does not map the Rust model to a runtime protocol,
implement any host/Temporal/store adapter, or prove an external effect.

## Exact source and preservation

- Repository: `newdarwindev/runspindle`.
- Merged source commit: [`e8cd5779968fede213443db13a591c992cd0e183`](https://github.com/newdarwindev/runspindle/tree/e8cd5779968fede213443db13a591c992cd0e183/fixtures/v1).
- Source path: `fixtures/v1`; version: `runspindle.fixture.v1`.
- Source manifest Git blob: `aa24016460196fb4e5fa0a335035394efb05eb12`.
- Source manifest SHA-256: `1dd35d045064056dd66e6da3619ff100549e1c3e81c928f076cfd4109a524d81`.
- Core subset manifest SHA-256: `4ece21a0af164e15ae29fa68adad32657dee0236d9395ea253326cd5d5cbbd34`.

Before export, all seven source fixture blobs were compared to the GitHub tree
at that exact merged commit. `fixturekit.py`, `contract.json` and `artifact.txt`
are byte-preserved. The source exporter filters golden fixtures and mutation
vectors to the three core kinds and creates the deterministic subset manifest.
The full source manifest is byte-preserved as
`fixtures/shared-v1.origin-manifest.json`; `fixtures/shared-v1.pin.json` records
this independently reviewed origin and exported-subset pin. The full source
bundle and sibling repositories are not needed at test time.

The kit is original synthetic Runspindle material reused at the owner's direction.
Its existing headers/notices remain intact; no third-party dependency or legacy
executable source was imported, and no new blanket license is granted. Source
legal context remains [LEGAL.md at the pinned commit](https://github.com/newdarwindev/runspindle/blob/e8cd5779968fede213443db13a591c992cd0e183/LEGAL.md)
and this repository's [legal status](../LEGAL.md).

## Local checks

Requires Python 3.9 or newer. No installation, network access, credentials,
sibling services or Git metadata are needed for fixture conformance:

```sh
python3 tools/check_shared_fixtures.py
python3 -m unittest discover -s tests -p 'test_shared_fixtures.py' -v
```

The consumer checks pin field bindings, exact origin/subset manifest hashes,
source manifest Git blob, complete subset inventory, all file hashes and
byte-preserved source-file hashes **before executing fixture code**. It compiles
the verified source bytes directly rather than allowing a Python bytecode cache
to substitute code. A reviewed source commit/pin remains a repository review
boundary; a self-consistent manifest is not remote source authentication.

All three positive fixtures and all nine published negative vectors run under a
Python socket guard. Independent expected identities and delivery receipt
observations come from the unmodified pinned fixture; the accepted artifact digest
is computed from actual synthetic bytes. Wrong version/host/digest, unknown success
and fabricated receipt mutations must raise the exact expected rejection. A
permissive validator mutation must make the consumer check fail.

The 11 Python tests also cover floating/changed pins, artifact/kit/manifest
corruption rejected before code execution, extra files, missing independent
receipt, changed operation identity, matching-but-unreviewed bytecode cache, and
DNS/connection/socket creation being denied. A fresh Python process runs the
checker from a temporary standalone copy with only fixtures and its checker,
without Git metadata or siblings; it has a 10-second timeout and temporary
resources are cleaned on ordinary success and failure.

The guard denies the exercised Python socket entrypoints. It is not an OS network
sandbox and cannot qualify untrusted/native adapters; future real adapter suites
still require the separately isolated runner/sandbox and explicit effect seams.
No actual transport, live provider or persistent process is started here.

Existing required checks remain separate:

```sh
python3 tools/check_scaffold.py
cargo fmt --all -- --check
cargo test --offline --all-targets
cargo clippy --offline --all-targets -- -D warnings
python3 tools/check_model_mutations.py
```

The local receipt [shared-fixture-tests.json](shared-fixture-tests.json) records
source base, input SHA-256 values, toolchain, exact commands and outcomes. This is
synthetic fixture consumer conformance, alongside the existing 20 Rust model tests,
not actual runtime producer/consumer conformance or full integration.

## Upgrades and remaining work

Upgrade the source commit only after reviewing upstream compatibility changes,
re-exporting the selected subset, preserving the new source manifest, updating
both manifest pins and rerunning all golden/negative vectors and component
checks. An exact 40-character commit is required; floating branches are rejected.
Do not automatically accept a regenerated manifest or upgrade sibling pins.

Core #1 remains open for actual fault-injectable Temporal/activity/host/browser
and delivery adapters, authenticated evidence, trustworthy hashing/authorization,
clocked host observations, reviewed retention/expiry and a real durable atomic
store with concurrency/fresh-process recovery. The fake schedule and receipt
helpers cannot satisfy those gates. GitHub CI, isolated integration, live
acceptance, deployment and migration completion remain unverified/not run.
