# Desired intent: Durable intent and effect coordination

Status: engineering direction and acceptance obligations, not a new implementation
claim, grant of authority, or replacement for existing versioned contracts.

## System intent and evidence boundary

Runspindle is intended to turn an authorized one-off task into a repeatable,
maintained workflow by composing scoped access, managed native/browser sessions,
explicit human takeover, and evaluated workflow candidates. The useful capability
is the composition: cross-application execution can retain identity and evidence
across interruptions, while approved examples can inform later DSPy experiments.
This is a design objective, not evidence of emergent intelligence, autonomous
self-improvement, deployed integration, or measured gains.

A workflow revision must identify its inputs, permitted effects, target bindings,
exception policy, acceptance criteria and resource budget. Repetition does not
create a recurring schedule or expand permission. Human intervention is first an
execution event; only a separately reviewed, consented and sanitized example may
enter a dataset. A successful evaluation is evidence for review, not authority to
promote, serve, spend, publish or act in another account.

## Component responsibility

Core should own the transition from a user-approved intent revision to a bounded,
reconcilable execution, not own website credentials or invent native sessions.
The desired Temporal layer coordinates durable admission, pending dispatch,
observations and cancellation while effectful adapters enforce their own target
and lease boundaries. Deterministic orchestration replay must never replay a
credential injection or external submission as an unguarded side effect.

## Interfaces and acceptance obligations

- Preserve immutable task/revision/operation/policy/host/workspace/lease bindings.
  Separate delivery deduplication from task identity; text similarity is not a
  safe request-deduplication key. Admission is not blanket future authorization.
- Persist dispatch intent before invoking an effect. A lost response remains
  unknown until authoritative reconciliation, not failed or safe to retry. No
  exactly-once external-effect claim follows from a unique internal workflow ID.
- A human exception suspends only the relevant work under explicit policy;
  resumption needs fresh target observations and valid authority. Feedback and
  candidate scores cannot modify grants or silently revise the active intent.
- Carry immutable artifact/candidate references and evidence state separately
  from delivery state. Reject stale revisions, conflicting receipts and changed
  pins; host unavailability must not silently select another computer.
- Future durable tests must cover worker/server restart, pending-dispatch recovery,
  duplicate admission, bounded retention/backpressure, cancel/completion races and
  unknown external outcomes using independently observed effects.

## Present scope

The default Rust models remain process-local. The optional `temporal-runtime`
feature adds a bounded development runtime with a single Temporal admission
registry authority per namespace, 1,024 retained delivery/revision records,
deterministic child dispatch and operation reconciliation. Real pinned SDK/server
tests exercise concurrent delivery, fresh worker/server recovery, cancellation
races and same-version history replay with independently observed scripted effects.

The [Runtime v1 mapping](runtime.md) documents its explicit semantic and Admission
JSON scope. This does not qualify native/core-host wire compatibility, trusted
native observation freshness, browser fencing or production authentication/expiry.
Host effects are scripted; browser, delivery and provider adapters are not
exercised. Production durability, competing registries, upgrade compatibility and
full-system/live acceptance remain blocked or unqualified. See [model contracts](contracts.md),
[runtime evidence](../docs/runtime-tests.json) and
[preparation and remaining gaps](../docs/TEMPORAL_RUNTIME.md).
