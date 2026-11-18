# F10: exact accepted-byte delivery, bounded offline acceptance

Criterion audit for [Core #5](https://github.com/newdarwindev/runspindle-core/issues/5).
The issue explicitly permits synthetic adapters and separately pinned isolated
integration. This audit does not declare production deployment or live delivery.

## Implementation and criterion mapping

All named regressions below are in [tests/outbox.rs](../tests/outbox.rs), unless
otherwise linked. Their peer ledger independently stores send/lookup calls,
original receipt binding and actual bytes in a different SQLite database.

| Original deliverable or criterion | Implementation and direct verification |
| --- | --- |
| Durable reservation/outbox, operation identity and unknown reconciliation | [Outbox](../src/outbox.rs) commits the reservation before send; `fresh_process_reservation_and_lost_ack_never_resend`, `lost_reply_reconciles_exact_bytes_without_another_send` |
| Exact digest, accepted revision, account/destination and external identity | Complete `DeliveryReceipt.request` equality plus bounded receipt ID; `forged_receipt_bindings_for_unchanged_request_never_establish_delivery` changes digest, artifact revision, intent revision, destination or account in send acknowledgement, readback, or both while the caller request stays unchanged (15 cases) |
| Concurrent workers/restart cannot create extra effects | `concurrent_process_owners_send_at_most_once`, `fresh_process_reservation_and_lost_ack_never_resend`; separately pinned actual Tether journal tests exercise lost replies, competing recovery and conflicting observations |
| Restart never regenerates accepted bytes or blindly resends | Original artifact BLOB and independent peer bytes/hash remain exact; immutable acceptance and retained-validation checks are reused, not recreated; `missing_journal_blocks_fresh_process_without_resend` and the new receipt matrix retain one send across reopening |
| Fabricated/wrong-digest/wrong-target receipt fails | `fabricated_delivery_receipts_do_not_pass_readback`, new unchanged-request receipt matrix, and `ambiguous_wrong_and_conflicting_receipts_never_overwrite_delivery` |
| Delivered requires verified evidence; unresolved stays unknown | A send acknowledgement alone is insufficient; independent unique readback must agree; contradictory evidence is quarantined. The new matrix permits later correct independent readback to resolve an invalid acknowledgement, never a forged observation |
| Deterministic positive/negative cases, independent forbidden effects and deliberate mutations | [Outbox mutation harness](../tools/check_outbox_mutations.py) removes journal-schema enforcement, independent readback, exact receipt-request binding, resolution authority and readback authority. Each requires a named behavioral assertion to fail, not a compile error |
| Fresh processes against actual persistence | Core SQLite child-process tests and actual Core/Tether journals exercise reservation, lost acknowledgement, concurrent owners and explicit resolution. Process crashes are not power-loss qualification |
| Versioned envelopes, invalid bindings, transitions and adapter responsibilities | [Wire transport](TETHER_WIRE_BRIDGE.md), [resolution protocol](TETHER_WIRE_RESOLUTION.md), [contracts](../specs/contracts.md), and shared conformance vectors specify exact immutable requests, bounded responses and unknown/conflict behavior |
| Bounded adapters and explicit authority/fault seams | Injected Transport/DeliveryAuthority/ResolutionAuthority; independently supplied clock/current trust in [AuthenticatedOutbox](AUTHENTICATED_DELIVERY.md); bounded rows/bytes and explicit failure points. SQLite transactions serialize local ownership; provider readback/identity guarantees remain adapter responsibilities |

## Cross-component evidence and prerequisite interpretation

[Core #17](https://github.com/newdarwindev/runspindle-core/issues/17) is the merged
bounded Core/Tether bridge implementation. The isolated lane uses exact Tether
`0393a700957bcb7dfe67754cebaf8fd98f3f05ac`, actual journal stores and parsers,
synthetic authority, and an independently counted SQL-backed HTTP fixture.
The fixture is not a production account or credential issuer.

Earlier Core #5 comments treated all of
[Tether #9](https://github.com/newdarwindev/runspindle-tether/issues/9) as a blanket
prerequisite. The criterion-level distinction is now explicit: Core #5's accepted
byte/destination/readback slice is implemented and exercised by the pinned lane;
Tether #9 still owns its broader rate-limit, partial-attachment, cancellation and
transport qualification obligations. This audit neither closes that issue nor
represents its remaining requirements as completed.

Shared synthetic fixture acceptance is recorded separately in
[composition #1](https://github.com/newdarwindev/runspindle/issues/1).
Composition hero scenarios and runtime-pin qualification remain their own work;
Core #5 is not a claim that the latest entire composition is qualified.

## Reproduction and boundaries

Use the existing pinned builder in `temporal/toolchain.json`, locked offline
Cargo dependencies, read-only Core/Tether source mounts and Docker network-none.
Set `RUNSPINDLE_TETHER_WIRE_SOURCE` to the explicit pinned mounted Tether path.
Run both normal and optimized Python modes without changing the Rust profile:

```sh
cargo test --locked --offline --features verified-authority,authenticated-artifacts --test outbox -- --include-ignored
python3 -m unittest tests.test_tether_wire_fixture
python3 tools/check_outbox_mutations.py
cargo clippy --locked --offline --features verified-authority,authenticated-artifacts --all-targets -- -D warnings
```

Exact candidate/tree, output hashes and observed results belong to the associated
PR receipt, not an inferred green status from this document. No live Slack/native
provider, production issuer custody, power-loss/filesystem qualification, arbitrary
upgrade compatibility or distributed atomic revocation is claimed. Host #8,
Core #18/#19, Tether #9 and composition acceptance retain their own obligations.
