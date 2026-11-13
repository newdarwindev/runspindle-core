# Core isolation coverage and unresolved adapter seams

Scope: local synthetic model test extension for [issue #1](https://github.com/newdarwindev/runspindle-core/issues/1).
This is a reviewable test plan, not an agreed runtime or shared wire protocol.
The original model-only extension below consumes no shared fixtures. A later
[pinned test-only subset](SHARED_FIXTURES.md) now exercises core fixture consumption;
runtime adapter mapping remains unagreed and composition issue #1 owns shared changes.

## Run locally

Requires preinstalled Rust/Cargo 1.94.1 with rustfmt/Clippy and Python 3.9 or newer.
No installation, provider, network service, credential or sibling checkout is needed.

- Model suite: `cargo test --offline --all-targets`.
- Deliberately broken models: `python3 tools/check_model_mutations.py`.
- Required structure/lints: `python3 tools/check_scaffold.py`,
  `cargo fmt --all -- --check`, `cargo clippy --offline --all-targets -- -D warnings`.

The model suite uses invented fixed references and integer enumeration; it reads no
clock, random generator, external transport or persistent store. The mutation tool
copies only Cargo metadata, `src/` and `tests/` into a disposable temporary directory,
uses offline Cargo with a 60-second limit per subprocess, and removes the copy on
success, failure or Python exception. It never modifies the checked-out source.
Baseline failure, compiler failure, zero selected tests and an undetected mutation
fail the mutation command; none counts as successful detection.

## Retained coverage inventory

All 16 existing `tests/contracts.rs` tests are retained unchanged:

| Tests | Existing model assertion |
| --- | --- |
| `opaque_ids_reject_empty_whitespace_urls_and_overlong_values` | Representative ID syntax and digest wrapper |
| `admission_distinguishes_transport_replay_revision_and_intent`; `transport_namespaces_do_not_alias_event_ids` | Event/revision/task/namespace distinctions |
| `conflicting_payload_host_workspace_policy_and_revision_reuse_are_rejected` | Delivery and intent conflicts; rejected event not consumed |
| `host_pin_never_fails_over_and_unknown_is_not_eligible` | Exact pins, offline/unknown comparisons |
| `lifecycle_requires_validation_acceptance_and_observed_receipt` | Evidence and delivery progression |
| `delivery_before_acceptance_rejects_without_mutation`; `state_transition_matrix_rejects_skip_repeat_and_regression` | Illegal transitions preserve records |
| `all_transitions_reject_changed_digest_identity_revision_or_policy` | Immutable reference binding |
| `acceptance_of_a_previous_validation_is_rejected`; `an_old_acceptance_cannot_accept_a_new_revision_with_identical_bytes` | Validation/revision reference binding |
| `duplicate_delivery_is_not_a_new_reservation`; `uncertain_delivery_requires_reconciliation_not_blind_retry` | Pending and uncertain replay decisions |
| `receipt_or_uncertainty_before_reservation_is_rejected` | Reservation precedes observed delivery |
| `delivery_id_and_destination_conflicts_reject_in_every_reserved_state` | One delivery identity/target |
| `a_delivered_receipt_cannot_be_replaced_or_regressed_to_uncertain` | Receipt conflict and terminal state |

`src/lib.rs` also retains the activation-fails-closed unit test.

The three new `tests/sequence_contracts.rs` tests add:

1. The complete ASCII alphabet at lengths 1 and 128, explicit 0/129/256 length
   boundaries and representative non-ASCII code points.
2. Rejected reuse of an existing delivery must not register another task/revision;
   subsequent valid registration still reports new. Revision inputs 0, 2 and
   `u64::MAX` preserve the model's current unordered numeric semantics. No new
   revision/supersession policy is standardized.
3. Every action prefix through length seven over validation, acceptance,
   reservation, uncertainty and observed receipt: 97,655 transitions. An explicit
   six-row transition table predicts outcomes separately from implementation
   branching; every step checks artifact/delivery state, immutable reference,
   evidence presence and whole-object equality for rejected/no-op operations.

Mutation checks independently require a named test assertion failure for:
uncertainty made retryable, the ID limit relaxed to 129, and rejected delivery
reuse consuming its attempted revision. These are test-sensitivity observations,
not security, durability or external-effect evidence.

## Proposed seams requiring agreement before adapter tests

| Seam | Required inputs and observed outputs | Faults and independent assertions |
| --- | --- | --- |
| Admission/outbox store | Authorized immutable request binding; expected record version; writer epoch; atomic admission, workflow/operation and outbox references; unique delivery key | Two fresh processes race same key or incompatible revision; crash before commit, after commit/before reply, corruption, expired writer. Read authoritative committed state independently. At most one authorized effect reservation; rejected writes preserve all prior records. Storage engine, serialization, locking/isolation and retention/expiry rules remain unselected. |
| Workflow/activity driver | Injected virtual clock and IDs, recorded schedule/event history, supported signal/cancel order, attempt identity, host/browser/delivery ports | Schedule is never execution proof. Lost launch/send reply enters unknown outcome; reconcile existing session/effect before any retry. Cancellation requested, acknowledged and terminal remain separate. Pinned Temporal SDK/server and replay/version policy remain unselected. |
| Content and evidence verifier | Immutable actual bytes; computed SHA-256; validator revision; policy and actor evidence; destination-specific grant | Independent byte mutation, stale validator, forged actor/approval, changed destination, mutable URL. Zero send calls after rejection. No hashing/authentication/authorization adapter exists in this crate. |
| Host observer and queue | Host/workspace/session binding; trusted observed time/version; injected clock/freshness limit; current writer/lease epoch | Offline pin queues; unknown/stale observations cannot execute; wrong workspace and old writer reject before launch. Reconnect reconciles the original actual session. Freshness/retention policy and native host adapter remain unagreed/unimplemented. |
| Delivery reconciler | Exact accepted bytes/reference; authorized destination; durable outbox entry and receipt observation | Restart after acceptance or lost send acknowledgement never regenerates accepted bytes or blindly repeats send. Positive/no-effect/inconclusive readback remain distinct. Destination adapter and authoritative receipt contract remain unselected. |

For every eventual seam suite, inject clock, IDs, fault points and transport/store
ports; use independent call spies with count/order and zero forbidden calls. A fake
may test the proposed contract only. Durable claims require the selected real store
and fresh processes; Temporal replay requires the pinned real local upstream.
Capacity and retention require a reviewed policy before eviction tests; silently
forgetting an old delivery must never authorize a duplicate effect.

## Open functional blockers

| Issue | Concrete prerequisite/gap preventing completion |
| --- | --- |
| [#2 F02](https://github.com/newdarwindev/runspindle-core/issues/2) | Reviewed shared identity/authorization contract; trusted admission identity adapter; selected atomic admission/outbox storage, revision/supersession and replay-retention policy. Current ledger is unbounded/process-local and does not return a durable job. |
| [#3 F03](https://github.com/newdarwindev/runspindle-core/issues/3) | Pinned Temporal SDK/server and replay policy; durable operation/outbox adapter; actual native session reconnect and browser reconciliation contracts; authoritative lease/cancel/freshness semantics. A host reference comparison does not execute or recover a session. |
| [#4 F09](https://github.com/newdarwindev/runspindle-core/issues/4) | Actual immutable content acquisition/hash adapter, validator/policy/actor trust source and atomic accepted-content store. Caller-supplied 32 bytes and evidence IDs are comparisons only. |
| [#5 F10](https://github.com/newdarwindev/runspindle-core/issues/5) | F09 accepted-content store plus destination authorization, durable unique outbox/writer fencing and reconciled actual delivery adapter. Separate `Artifact` clones can each reserve; the local model provides no global effect uniqueness. |

Issue #1 remains open: this extension completes additional existing-model coverage
and proposes seams, but fake adapter suites, reviewed shared fixtures, actual store,
hashing/authentication, fresh host observations and Temporal qualification are
blocked/not run. No operational gate is weakened. A local pass is never GitHub CI,
isolated integration, live acceptance, deployment or selected-source migration.
