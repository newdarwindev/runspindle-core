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
