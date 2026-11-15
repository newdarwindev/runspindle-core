# Authorized conflict resolution (unqualified candidate)

Partial core issue17 implementation, based on core candidate
`48002b65868dc69fb7e35cc957d02e9a06c7208a`. This extends the existing Rust SQLite
outbox and immutable accepted BLOB storage. No second registry, provider journal,
new transport, service, trust issuer or default authority is added.

Delivery and reconciliation now recheck current DeliveryAuthority before and
after readback, and settlement checks again after the SQLite write lock. A revoked
result cannot mark delivery complete. An uncertain reservation remains fenced;
no subsequent call can retry send.

ConflictResolution binds the entire quarantined DeliverySnapshot (including its
request, generation, prior acknowledgement and receipt), the chosen exact receipt,
and an opaque bounded authorization reference. ResolutionAuthority is a separate
trusted application interface. Deny implements it by returning false; ordinary
admission or delivery authority does not implement or imply resolution authority.
The reference is not a credential: the supplied verifier must check current
permission for this exact proposal, including destination, policy and evidence.

Outbox::resolve performs read-only lookup and requires precisely one matching
receipt. It checks both authorities before lookup, after lookup, and after taking
the SQLite write lock. A stale full snapshot, concurrent winner, ambiguous/wrong
readback, invalid proposal, unavailable authority or audit capacity denies without
changing the quarantine. Successful resolution atomically writes an immutable
resolution record, advances generation once, clears conflict and marks the chosen
receipt delivered. It never calls Transport::send. The original acknowledgement
and accepted bytes remain unchanged. A resolved receipt is the reference for later
contradiction checks, rather than the superseded transport acknowledgement.

An additive outbox_evidence table retains first conflict observations and each
resolution proposal. UPDATE/DELETE triggers prevent normal mutation. Existing
outbox state values/version remain readable; no destructive migration occurs.
Records are limited to 1 MiB and the table to the existing 1024-record capacity.
Conflict evidence exhaustion or oversize commits the quarantine flag and advances
generation, then returns an explicit incomplete-evidence error. No oversized row
or extra capacity is allowed. A quarantined generation without a conflict audit
record denotes incomplete evidence (including historical records); callers must
not infer complete evidence from a retained receipt. Restart and later benign
readback cannot clear that fence. Resolution still requires its complete bounded
audit record, separate authorization and exact readback. Storage/transaction
failures remain errors, not proof of durable quarantine. No pruning or permission
to resend is introduced.
Historical quarantines may lack the new conflict record; their entire retained
snapshot is still recorded if explicitly resolved.

Trust limits: callbacks must be fresh, bounded and non-reentrant. They are not
serialized with a remote authority or provider; the actual adapter must fence
remote effects. The local database is component-owned, not protected against a
hostile writer. This API does not authenticate a provider receipt itself, nor
replace core #16 exact-destination authority work. Tether #15's provider journal is a
future dependency; no Python/Rust bridge or live provider is connected here.

Verification commands (Rust stages not executed in this cloud workspace):
- cargo fmt --all -- --check
- python3 tools/offline_rust.py test --features delivery-runtime --test outbox
- python3 tools/offline_rust.py clippy --features delivery-runtime
- python3 tools/check_outbox_mutations.py
- python3 tools/validate.py

New Rust tests cover readback revocation, separate resolution permission,
wrong/ambiguous scope, stale generations, immutable evidence and byte preservation,
audit exhaustion/oversize fencing, transaction rollback, process death after lock acquisition and racing fresh-process
resolvers with independent send counts. Deliberate guard mutations must fail named
behavioral tests, never count compilation failure as detection. These tests are
written, not runtime-qualified until the pinned toolchain/native lane executes.
Issue #17 and its parent #5 remain open. No Temporal/native/live acceptance is claimed.
