# Immutable artifacts and authorized acceptance

The optional `artifact-store` feature implements the reviewed
`runspindle.integration.v1` artifact/validation/acceptance interfaces from
composition `96299842505ce7662178b7c7b00fd31f6c2754f0`. It is separate from the older
process-local model in `artifact.rs` and from delivery authority. It adds no
provider, credential resolver, native app-server or live activation.

`Store::put` stores actual bytes as an immutable SQLite BLOB and computes SHA-256.
Identity includes the complete seven-field Binding, artifact ID and positive
artifact revision; no partial task/host identity aliases another artifact. Exact
repetitions return the same reference; changed bytes for that identity conflict.
`Store::validate` rejects malformed validator/policy metadata before invoking the
injected validator on the stored bytes. The immutable validation key binds the
exact artifact, validator revision and policy. A different result cannot overwrite
it, and a passing boolean supplied by a caller cannot substitute for a stored
validation record.

`Store::accept` defaults to an injected denying authority. Inside an immediate
transaction it verifies actual bytes, exact passing validation, complete authority
and one acceptance per task/revision. Acceptance and its byte reference commit
atomically, with foreign keys to retained artifacts. Concurrent conflicting
acceptances cannot both succeed. An identical accepted reference reconciles after
restart without reading a generator or regenerating work. Validator revision and
policy changes never inherit acceptance. Acceptance grants no destination authority.

All requests require the explicit integration version; the core SQLite schema is
version 1 and rejects unknown or unversioned nonempty stores. Each artifact is at
most 1 MiB, with at most 1,024 artifacts, 1,024 validations and 1,024 acceptances.
New identities reject at capacity; exact known entries reconcile. SQLite has FULL
synchronous WAL commits and a 524,288-page main database bound. Immutable-row
triggers reject update/delete. File-system/power-loss, corruption recovery,
production retention and authentication are not qualified by process-crash tests.

The core-owned database is separate from host coordination. Existing dependency
pins include rusqlite 0.32.1 and libsqlite3-sys 0.30.1, exact bundled SQLite source
bytes and license notices. Component tests independently hash binary fixture
bytes, exercise missing/failed/forged validation and unauthorized acceptance,
changed bytes/revisions/policy, concurrent conflicting accepts, bounded validation
retention and fresh-process recovery after acceptance commit. Behavioral mutations
skip validation lookup or byte-digest comparison and must fail assertions.
