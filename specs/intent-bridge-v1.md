# Semantic intent bridge candidate

Related: Tether #8 and parent #3. This is an isolated synthetic candidate, not a
production ingress, issuer, native adapter or complete live bridge. No legacy
v1/v2 workflow registration or historical command sequence is reinterpreted.

`runspindle.intent-admission.v1` wraps a scoped admission and explicit semantic
event. The event binds source app/account/actor/workspace/channel/thread/message,
transport event ID, revision, create/edit/delete kind, content SHA256 and exact
predecessor request/full-wire hash. Only digests, never raw content or signatures,
enter workflow history. The request normalized digest is the canonical semantic
event hash. The separate authority-v3 signing domain commits the entire wire.
The configured helper actor must equal the source actor. New grants are required;
old ingress grants are not silently promoted.

Task ID is `tether-task:` plus SHA256 of canonical JSON array
`[app,account,workspace,channel,thread,message]`. Actor is deliberately excluded:
actor substitution reaches the same intake and conflicts with retained source,
not a second executable task. The intake workflow ID is `intent-` plus SHA256 of
`[app,account,workspace,task_id]` in the one configured Temporal namespace. This
lane does not automatically migrate arbitrary legacy task IDs. Exact destination
is independently derived from `[app,account,workspace,channel,thread]`.

## Atomic chain admission

`runspindle.intent-batch.v1` contains full canonical `intents` from revision1 to
latest. The complete chain is bounded to64 executable revisions and one final
delete, at most1MiB. Source, task, host, workspace and lease cannot change; event
and operation identities are unique. Every predecessor request and full-wire hash
must match. A delete is final and contains the empty-content digest.

Current authority for the latest full wire commits the exact transitive chain.
Only latest intent may execute. Earlier missing revisions are retained explicitly
as `superseded_without_execution`, never as successful or terminal effects. The
existing Core history must be an exact prefix. All validation and current-state
ordering checks occur before any mutation, after async authority completion.
Stale/conflicting batches cannot replace history or restore pending execution.

Latest edit rejects while an existing operation is active or unresolved. Latest
delete additionally requires current Cancel authority, atomically clears pending
work, retains a terminal intent tombstone, and requests cancellation of the exact
active ancestor. A delete never becomes a Launch, even if its assertion includes
Dispatch. Cancel/Reconcile guard verifies the latest grant and exact ancestor
admission proven by the immutable same-source/host/lease hash chain. It does not
require an expired or revoked old grant. Fresh authorization is checked at the
activity boundary; trust unavailability remains deny-only.

## Scheduling and cancellation boundary

When a single or batch delete clears a queued revision, the query snapshot
retains that revision as `superseded_without_execution`. A batch records the
previously queued revision before any newer skipped revisions. Once scheduling
has consumed a queued revision, deletion leaves the existing operation and its
cancellation or terminal evidence to the operation state machine.

Deletion before launch scheduling prevents a new launch command. A launch already
scheduled before deletion is an in-flight external effect, not retroactively
undone: it remains pending/unknown, is reconciled, then cancellation is requested.
Cancellation acknowledgement is not terminal observation. No atomic provider
fence or exactly-once external effect is claimed. The new synthetic worker has
bounded observation/launch barriers to distinguish these two races in tests.
The existing legacy deterministic loop chooses exactly its original activity
branch because its new optional intent accessor defaults to None.

## Receipt and recovery

`admit_intent_batch` returns admission status, exact workflow ID, final revision
and submitted batch SHA256. `intent_receipt` independently queries retained Core
state and reports `retained` only for an exact full prefix match. This acknowledges
retained metadata, not execution, cancellation or delivery. Lost submission replies
can therefore be reconciled read-only without stale grant replay or blind effects.

The Tether producer and atomic semantic journal are separately pinned prerequisites.
They must authenticate transport and retain source/event uniqueness before issuing
an export. A pure codec is not authentication. Raw Slack change/delete metadata is
not editor/deleter authority; that provider mapping remains explicitly unsupported
unless a separate current-authority adapter verifies it. No production activation,
new provider credentials, persistent services or host security changes occur here.

## Candidate verification

`tests/intent.rs` checks wire/semantic binding, current authority, target
substitution, retained ordering, capacity deletion, atomic supersession and actual
scripted call/effect observations. `tools/test_intent_bridge.py` is the bounded real
Temporal/SQLite plus Tether integration, with fresh-process recovery/history replay.
The complete exact-source qualification at `9358554cc615c6fe027be0910e4d09fbf392899b`
and corrected Tether `f1392fbf4861c4667732f351bd5d466d34c5ece8` passed. See
[the evidence receipt](../docs/intent-bridge-qualification.json) and
[acceptance mapping](../docs/INTENT_BRIDGE.md). Independent source/security and exact-evidence review found no blocking findings. Existing runtime/source
reports for other lanes are historical, not evidence for this lane.

Reproduce from a clean candidate and the exact Tether pin, with prepared pinned
runtime/Temporal artifacts and pinned quality tools on PATH:

```sh
PYTHONDONTWRITEBYTECODE=1 \
RUNSPINDLE_TETHER_V1_SOURCE=/path/to/legacy-tether-pin \
RUNSPINDLE_BUILDER_ARCHIVE=/path/to/verified-builder-closure.tar.gz \
/usr/bin/python3 tools/qualify_intent_bridge.py \
  --platform /path/to/pinned-platform \
  --platform-prepared /path/to/prepared-temporal \
  --composition /path/to/pinned-composition \
  --tether /path/to/pinned-tether \
  --prepared local/runtime-prepared --report-dir /path/to/report
```

The legacy Tether test-reference pin is `040b71f` (full revision checked by Tether's
own schema-upgrade suite). A renderer skip in its pre-existing visual suite is not
semantic or visual acceptance. The aggregate also runs the existing13-phase Core
qualifier, full Tether tests in normal/optimized mode, network-disabled independent
intent/batch golden conformance, repeated actual-journal integration, and deliberate
failure/SIGTERM cleanup probes. Deliberate mutations must fail actual assertions
and restore exact source bytes. No baseline weakening or hosted fallback is used.
