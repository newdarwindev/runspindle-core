# Exact-byte Tether wire transport

`outbox::Transport` now supports `send_observation` and `lookup_observation`
with explicit conflict evidence. Existing implementations inherit their previous
receipt-only behavior. A trusted conflict is durably quarantined without inventing
a provider receipt; conflict resolution still requires separate authorization and
an exact unique non-conflicting readback. No outbox schema or historical workflow
command path changes.

The opt-in `delivery-runtime` module `tether_wire` implements this seam for one
immutable `DeliveryRequest`. Construction reads actual accepted bytes from the
Core Store. `send` checks those same bytes, and every exchange binds the complete
canonical envelope, payload, action and version. The response must echo SHA-256
of that request and the `slack_wire_content_observation` evidence class. Only
`content_confirmed`, a valid file/message identity pair, and no conflict can
produce a receipt. Its stable ID hashes that real pair. Core still independently
calls lookup before settling a send acknowledgement. Unknown, malformed,
synthetic, unbound and conflicting results never become Delivered.

## Explicit trust boundary

`Peer` is an injected trusted local adapter, not authentication or a deployment
launcher. There is no default peer, subprocess, network endpoint, credential,
automatic retry or production activation. A peer implementation must provide a
bounded exchange and route only to the intended Tether instance. The full-request
hash binds the response but is not a signature. Core's DeliveryAuthority and
Tether's fresh admission/accepted-artifact/route/wire authority checks remain
required. Their application wiring is not supplied here. Filename/text are
trusted presentation inputs; Tether's immutable journal additionally rejects
changes to their payload digest across restarts. No claim of atomic remote
revocation or distributed exactly-once execution is made.

The version is `runspindle.tether-wire-bridge.v1`. Requests contain exactly
`version`, `action` (`send` or `lookup`), `envelope` and `payload`. Payload contains
`text` and one `attachment` with `attachment_id`, `filename`, and lowercase
`content_hex`. Responses contain `version`, `request_sha256`, `evidence_class`,
`state`, `file_id`, `message_id`, and `conflict`. No upload acknowledgement or
synthetic legacy receipt is accepted as content observation.

## Isolated cross-component verification

`tests/fixtures/tether_wire_peer.py` is a test-only bounded stdio fixture. A trusted
expected file is written independently before each run; incoming requests cannot
create or broaden its synthetic authority. It uses actual Tether Admission,
WireDeliveryJournal and UploadJournal plus the independent SQLite HTTP peer from
Tether tests. Socket connections are rejected. Production receives none of these
fake authorizers or executable-selection behavior.

The explicit integration tests are ignored in the default suite, and must be
invoked with a reviewed exact Tether source checkout:

```
RUNSPINDLE_TETHER_WIRE_SOURCE=/path/to/pinned/tether \
  cargo test --offline --features delivery-runtime --test outbox -- --include-ignored
```

They use fresh Core processes and fresh Tether peer processes, retaining actual
SQLite journals between runs. Scenarios cover healthy delivery, lost completion
acknowledgement, lost bridge response after commit, wrong thread, changed message
identity and revoked synthetic authority. They independently assert exact stored
bytes and one allocation/upload/completion in the HTTP peer ledger. They do not
qualify Slack credentials, live provider semantics, production identity issuance,
native sessions, power loss or storage-controller failures. Exact source revisions
and execution results belong in a separate verification receipt after review.

## Reviewed execution candidate (2026-11-17)

The [qualification receipt](evidence/tether-wire-20261117/qualification.json)
records Core `da1980b` and Tether `d6138a833773691009863f686f235283d7a82723`,
clean before/after, with source and log hashes. All 33 outbox tests ran, including
all three opted-in cross-component tests (six scenarios), with no skips. The
same binary passed with Python optimization enabled; this is not an optimized
Rust build. The independent expected-binding negative test rejected six mutations
in both Python modes before any store was created. Rust source was unchanged
from compiled commit `98a8f62`; subsequent commits only changed Python tests.

A separate all-features compile attempt exhausted local disk and is **not** a
passing result. Only owned regenerable compiler cache was reclaimed. No live
network, credentials, production authority wiring or native integration was run.
The Python fixture denies socket construction but is not an OS network sandbox.

Core #17 remains open: the integrated Tether journal retains sticky conflicts and
this bridge cannot clear them. Existing general Core conflict resolution has its
own tests, but does not prove end-to-end authorized Tether conflict resolution.
No alternative peer is used to manufacture that missing evidence.

The final Python-only typing/import-discovery correction passed strict mypy and
Ruff with isolated import sorting; the full isolated checks were rerun after it.
Earlier receipts remain in Git history, not counted as final-head evidence.

The later [explicit resolution lane](TETHER_WIRE_RESOLUTION.md) adds bounded
append-only decision recovery. The earlier receipt above remains historical;
only the separately pinned resolution receipt covers that additional behavior.
## Reconciliation with current ingress hardening

The bridge discards callback and response-parser diagnostic content at its public
boundary. Peer failures and malformed observations return fixed categories without
retaining the original cause chain. A synthetic canary regression exercises both
paths. This does not redact valid payloads or qualify an arbitrary injected peer;
the trusted-peer, explicit authorization and exact-readback requirements remain.
