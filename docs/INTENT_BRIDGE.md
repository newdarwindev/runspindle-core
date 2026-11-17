# Bounded semantic bridge qualification

Related: [Tether #8](https://github.com/newdarwindev/runspindle-tether/issues/8),
[parent #3](https://github.com/newdarwindev/runspindle-tether/issues/3),
[Core PR #32](https://github.com/newdarwindev/runspindle-core/pull/32), and
[Tether PR #39](https://github.com/newdarwindev/runspindle-tether/pull/39).
No parent or production activation is implied.

## Exact tested candidate

- Core: `9358554cc615c6fe027be0910e4d09fbf392899b`.
- Tether: `f1392fbf4861c4667732f351bd5d466d34c5ece8`.
- Platform: `398c74766b9ec9f7203256e17dea34f1b15cac4c`.
- Composition: `ba5a102d7a2115ec9e00f748236ecbfddaab4c21`.

[Machine-readable evidence](intent-bridge-qualification.json) preserves exact
source fingerprints, source/config/fixture/artifact hashes, log hashes, repeated
integration reports and the earlier failed attempt. This publication adds evidence
and status documentation only; it does not retrospectively call the documentation
commit the tested source. Reproduce with the single aggregate command in
[the protocol](../specs/intent-bridge-v1.md). The runtime build/toolchain pins remain
in [runtime-build.json](runtime-build.json) and `temporal/toolchain.json`.

| Check | Observed result |
| --- | --- |
| Core Python | 108 passed |
| Composition Python | 357 discovered, 3 existing skips |
| Rust verified-authority | 96 passed, including 11 semantic tests |
| Rust fmt / strict Clippy | Passed |
| Tether normal and optimized | 425 discovered each, 1 existing visual-renderer skip each |
| Static-quality debt gate | Passed, 407 existing diagnostics, 0 new, baseline unchanged |
| Deliberate Core mutations | All 5 killed by behavioral assertions; exact source restored |
| Cross-language canonical intent/batch | Six independent vectors and eight negative cases passed, network disabled |
| Legacy/scoped Temporal integrations | Four runs passed, including fresh SQLite recovery and replay |
| Actual Tether journal to Core | Two runs passed, each 6 launches and 3 cancels |
| Success, failure, SIGTERM cleanup | Passed for owned containers, volume, network and temporary state |

The existing renderer skip is not visual acceptance. The aggregate does not claim
Tether's separate image-bound all-language/static gate; those inherited findings
remain in the producer PR. No absent check is silently treated as a pass.

## Acceptance mapping

- Exact app/account/workspace/channel/thread/message/actor and destination bind the
  semantic wire. Current v3 assertions bind the full wire, and generic source task
  IDs are deterministic. Wrong scope, forged evidence and substituted payloads
  deny before independent forbidden adapter/effect observations.
- Tether authenticates normalized transport before durable ingress. The new
  journal atomically retains ingress and full semantic wires, rejects provider
  event reuse across tasks or delivery namespaces, and fails closed on ambiguous
  historical ownership. Existing raw Slack create authentication is tested locally;
  raw Slack edit/delete metadata is not accepted as editor/deleter authority.
- Duplicates, stale revisions, changed source identity, incomplete/conflicting
  prefix, wrong predecessor and resurrection after delete are rejected or retained
  idempotently. Mirrored-output rejection stays in the ingress path; the new
  semantic export cannot bypass its retained source and mirror checks.
- A complete latest chain imports missing historical revisions as explicitly
  superseded metadata, not successful effects. A terminal delete clears pending
  execution and has reserved capacity even after all executable slots are used.
  Active/unknown edits are not silently queued.
- New current delete authority can cancel the exact prior active operation even
  when its old grant was revoked. An already-scheduled launch may still occur;
  reconciliation and cancellation follow. Cancellation acknowledgment never proves
  terminal outcome. No atomic provider dispatch/revocation fence is claimed.
- The actual journal exits immediately after a real SQLite commit, then reopens in
  a fresh process and retains offline edit/delete revisions. Atomic Core admission
  produces zero stale launches. Lost receipt recovery checks actual exact Core
  prefix readback before acknowledging retention. Later prefix receipts cannot
  acknowledge a newer pending head. Worker/server restart and history replay do
  not add ledger effects or invent external completion.

## Failed attempt and correction

The first corrected-Tether aggregate failed in the unchanged legacy authority
harness when it queried a newly admitted child before Temporal had created it.
The exact expected-child `Workflow not found` now raises a dedicated pending
exception retried only by the existing bounded poll. Other IDs, permission,
timeout and JSON errors still propagate. Four new tests passed normally and
optimized under independent review. No runtime code or effect/outcome assertion
was weakened. The full final aggregate then reran every phase and passed.

## Remaining boundaries

This is isolated component/integration evidence with synthetic identities and
issuer material, a real disposable Temporal/SQLite runtime and a scripted native
peer. It is not real Slack, native-session or production qualification. A pure
codec or retention receipt is not authority, delivery or completed execution.
Production needs reviewed actor mapping for actual edit/delete events, a real
issuer/key-custody/revocation deployment, controlled Temporal ownership, qualified
native/delivery adapters and explicit bounded live authorization. No MacBook,
Codex task, live provider, credential/access change or persistent service was used.
