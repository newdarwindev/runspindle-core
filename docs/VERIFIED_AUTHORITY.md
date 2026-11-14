# Offline runtime envelopes and authority

Candidate scope: four Python/Rust wire mappings and an offline Ed25519 admission
lane. Production, credentials, providers, paid evaluation, browser, console,
source migration and H08/Langfuse remain deferred. Independent review was waived
by the user; no independent review occurred. Required GitHub checks and merge
qualification are recorded separately from local evidence.

Prepare only the pinned dependencies described in temporal/toolchain.json and
docs/runtime-dependencies.json. The runtime never acquires network dependencies.
From core, on the isolated local Linux/Docker lane:

```
python3 tools/prepare_temporal_runtime.py --features verified-authority
python3 tools/qualify_verified_authority.py --platform /path/to/pinned/platform --platform-prepared /path/to/temporal-prepared --composition /path/to/composition --report-dir /path/to/private/reports
```

The qualification command verifies the committed prepared artifact source/image
binding, scaffold, Python/Rust suites, Rust formatting and strict Clippy, deliberate
mutations, independent wire producer/consumer mappings and fresh Temporal/SQLite
recovery. It fails on missing checks/tools. It creates disposable services and
synthetic ephemeral credentials only; there is no deployed issuer or provider.
Reports contain source/config/vector/toolchain hashes and bounded results.

Historical regression commands (same pinned prepared image):

```
python3 tools/test_temporal_runtime.py --platform /path/to/platform --platform-prepared /path/to/temporal-prepared --core-prepared local/runtime-prepared --report /private/reports/runtime.json
python3 tools/test_admission_checkpoints.py --platform /path/to/platform --platform-prepared /path/to/temporal-prepared --core-prepared local/runtime-prepared --prior-core /path/to/prior/core --prior-prepared /path/to/prior/prepared --report /private/reports/checkpoints.json
python3 tools/test_host_runtime.py --platform /path/to/platform --platform-prepared /path/to/temporal-prepared --core-prepared local/runtime-prepared
python3 tools/test_delivery_runtime.py --platform /path/to/platform --platform-prepared /path/to/temporal-prepared --core-prepared local/runtime-prepared
```

`authenticated-worker http://temporal:7233 /effects /effects/authority-helper.json`
registers only authenticated entrypoints on the authenticated queue. Existing
`worker` registers historical development entrypoints. CLI commands are
start_authenticated, admit_authenticated (canonical wire string),
authenticated_registry, authenticated_operation and cancel_authenticated (the
same canonical wire). The replay command registers both versions. The explicit
synthetic-issuer command only accepts a disposable /effects directory; it is a
test fixture generator, never a credential provisioning or production tool.
