# Core-owned browser control: bounded component

Source implementation with synthetic component tests; not an operational migration receipt.
Browser must own no canonical control storage. Keep the existing Core-owned
BrowserRegistry file; add only a control-head compare-and-swap row pointing to an
immutable transition operation. Do not copy holder/history into another ledger.

## Boundaries selected after independent design critique

- Control records use a reserved typed intent domain, distinct from page-effect
  epochs. Generic dispatch, reconcile and snapshot APIs reject this domain.
- A control transition contains exact immutable native/session/frame/origin scope,
  source/destination holders and predecessor operation/digest/generation. Validate
  the complete bounded chain, no gaps/forks/changed predecessor, before admission.
- Reserve the new operation and move its head in one BEGIN IMMEDIATE transaction
  before invoking the external fence. Unknown, rejected or conflicting heads block
  input. A submission acknowledgment never activates the destination.
- Initialize permits only an explicitly authorized agent owner, with fresh native
  backend proof; absence of a row is not permission. Takeover is agent-to-human.
  Handback is human-to-agent and requires current authenticated source release.
- Reconcile only the current reserved head; never repeat an uncertain fence or
  mint a fresh operation to retry it. Old confirmed control records cannot be
  changed through generic or subsequent historical reconciliation.
- Input admission needs exact current holder/epoch plus separately authenticated
  native input-fence authority. A historical active snapshot is not a capability.
  A Core-owned input transaction can hold the writer lock across a bounded helper
  invocation; an application may use a second connection to the SAME canonical
  file while the outer effect registry has no transaction held. No second store.
- Preserve conservative existing exact scope semantics; cross-origin/session/frame
  rebind is not silently added. Capacity is bounded with separate control budget;
  exhaustion must deny new grants, not bypass fencing. Out-of-band revocation can
  stop authority but is not an unrecorded ownership transfer.

## Migration safety

Versioned schema upgrade must be explicit, atomic and non-destructive, preserving
all existing operation IDs, payloads, outcomes and generations. Reserved-domain
collisions or unknown schema refuse without mutation. No deployed upgrade runs in
this work. Legacy Browser owners remain authoritative until a verified one-way
cutover: quiesce all old processes/input paths, bind the exact legacy scope,
operation IDs and epochs, prevent the old owner from reopening, then reconcile
under the new owner. Merely importing rows or renaming code is not cutover.
No deletion, backup, automatic legacy copy or dual-authority fallback is allowed.

## Required bounded tests

Schema refusal/atomic upgrade and unchanged existing operation rows; typed domain
separation; initial proof/release denial; unknown crash/restart and no repeat
fence; fresh readback activation; wrong predecessor/gap/fork/corruption; concurrent
transitions; input/transfer serialization; stale authority after waits; scoped
capacity denial; legacy owner refusal before cutover. All initial authorities and
native fence backends remain synthetic until separately qualified. Actual
production Host/browser ownership and Actions CI are independent acceptance gates.

## Implemented surface and explicit limits

`browser_control::{Transition, Authority, InputAuthority}` and
`BrowserRegistry::{upgrade_control_schema, dispatch_control, reconcile_control,
historical_control, with_control_input}` implement the bounded component.
The schema-v2 head table holds only browser/scope/operation pointers, not a second
holder or history ledger. Its referenced operation is the canonical transition.
A current unknown readback after terminal evidence sets sticky quarantine while
preserving the historical outcome; it cannot reactivate input. Quarantine recovery
is deliberately not implemented: do not retry with a new operation identifier.

Schema v2 is intentionally incompatible with pre-upgrade Core binaries, whose
schema check accepts only version 1. Rollback by decrementing the schema marker
is forbidden. This is NOT an owner-cutover mechanism for old Browser processes.
No legacy import/backfill, retirement marker, Host adapter or native drain/fence
implementation is supplied here. Those are mandatory reviewed follow-on gates.
Do not call initialization to adopt an existing legacy session or invent epoch 1
for it; preserve legacy IDs/epochs during an explicit later migration.

Authority callbacks are trusted synchronous, non-reentrant observations of a
real externally held native fence; a bool verifier by itself cannot synchronize
browser input. The SQLite writer lock serializes Core admission only, not native
input outside that lock. Same-owner SQL access is not an authentication boundary.
The caller must bound transport/callback lifetimes and preserve unknown outcomes
on cancellation or authority loss, including effects before a failed return.

Synthetic tests include two competing Core connections, current-input/transfer
serialization, additive upgrade rollback, unchanged legacy operation records,
no-refence on reopen, namespace denial, malformed metadata/pointers/payloads,
readback quarantine, and the independent 128-control reservation budget. Tests use
disposable SQLite files and fixture peers only. They do not prove native fencing,
actual legacy-owner refusal, production cutover, or Actions CI execution.
