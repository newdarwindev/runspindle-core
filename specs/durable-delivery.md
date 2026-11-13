# Durable delivery outbox and H07

The explicitly versioned `runspindle.integration.v1` DeliveryRequest binds an
accepted immutable byte reference, exact account/destination IDs and a distinct
stable delivery operation. Core's separate SQLite database retains at most 1,024
outbox operations; unknown work is not evicted. Default-deny injected delivery
authority is independent of acceptance authority and is checked before reservation
and immediately before sending. There are no credentials, provider calls or real
Tether transport in this implementation.

An immediate transaction reserves each operation before transport invocation.
Only a newly committed reservation can reach send. Existing reservations always
reconcile, even after process death before a send actually happened. Absence is
not permission to retry. Lost replies become Unknown; the outbox never regenerates
accepted work or resends. Bytes come from the retained accepted BLOB and are hashed
again before dispatch. Request binding cannot change during updates.

A send acknowledgement alone is insufficient: authoritative transport readback
must return exactly the same bound receipt before the outbox marks Delivered.
A lost acknowledgement is reconciled through the same read-only interface.
Receipts echo the exact operation, acceptance and destination plus an observed
receipt ID. Zero/multiple/mismatched readbacks remain Unknown; conflicting receipts
cannot overwrite an existing Delivered receipt. State/receipt updates compare and
swap the persisted generation. Contending reconciliation may make an original
owner stale; another readback resolves it without another send.

The `delivery-runtime` feature registers a single-attempt DeliveryWorkflow and
activities around the real outbox. Different Temporal workflow IDs sharing one
stable delivery operation still share its outbox reservation. Temporal replay does
not resend. Unknown snapshots remain separate from actual delivered receipts.
The H07 scripted destination owns an independent SQLite call/receipt/byte ledger.
It deliberately fails duplicate sends and supports lost acknowledgements,
process interruption and contradictory/ambiguous readback for tests. This is a
synthetic transport, not a replacement implementation of Tether; its selected
source, real transport and rights qualification remain separate blockers.

The development binary's explicit `/effects` artifact mode stages actual bounded
fixture bytes and validates them, then accepts a supplied exact validation only
through the injected synthetic grant. Acceptance and delivery are separate fresh
processes. The test removes the generator input before acceptance and delivery;
accepted bytes survive entirely in the real store. All inputs and emitted metadata
are synthetic. Public operational activation remains blocked.

H07 independently exercises process death after validation, after acceptance,
after delivery reservation before send, and after destination persistence before
acknowledgement. Fresh workers/server processes reconcile on the same stores.
Python independently hashes the actual destination bytes and checks ledger order,
send count, exact receipt bindings, duplicates, conflicting receipts and replay.
Seeded repeats compare normalized outcomes. Rust tests add concurrent process
owners, bounds, malformed/unauthorized requests and changed byte/target bindings.
A mutation that accepts a fabricated acknowledgement without readback must fail.

The first valid send acknowledgement is retained separately from a confirmed
receipt. A different valid receipt from readback creates a durable conflict fence,
including when the first readback was unavailable. Restart and repeated identical
contradictory readback cannot clear it. The outbox preserves any already confirmed
receipt and exposes the conflict flag; new workflows cannot report delivery
success while fenced. No automatic conflict resolution is implemented.
