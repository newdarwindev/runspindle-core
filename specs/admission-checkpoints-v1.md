# Authenticated admission interruption and specified-version upgrade candidate

Supports Core #2. This extends the existing synthetic SDK/client checkpoint
infrastructure, not the deterministic workflow command sequence. No activation
file/network I/O is added to workflow code. Worker checkpoint selection requires
an explicit CLI point, exact workflow type and exact workflow ID; unsupported
combinations fail closed. Replay and unknown activations cannot trigger a stop.

## Persisted boundary, not a fictional commit gap

- `admission_commit`: update-completed command generated, before its workflow-task
  completion is sent to the server. This is not proof admission is durably committed.
- `before_child_creation`: child-start command generated, before server commit.
  Independent server history must contain zero started children.
- `after_child_creation`: server-resolved successful child start, before the worker
  processes that activation. Independent history must contain one started child.
- `before_reply`: client received admitted update response, then deliberately exits
  without returning it. Child creation may follow later; no child-start claim is
  inferred from the response alone.
- Intent `before_dispatch_scheduling`: exact namespaced Launch activity generated
  before completion; independent history confirms zero durable launch commands.

Admission mutation and child intent may share a workflow-task transaction. The
harness does not claim an impossible separately durable gap. It records observed
pre/post child counts, admission result, retained revision count and history hashes.
Revocation during downtime always requires zero external launches, whether the
precommit request recovers as admitted-but-dispatch-denied or denied with no retained
revision. Cancellation acknowledgment remains separate from terminal observation. A revoked
Launch command regenerated on replay may be rejected by the helper while the
workflow conservatively retains Unknown; zero observed effects do not turn that
state into invented terminal completion.

## Specified-version upgrade

Prior source commits, executable digests and immutable image IDs are pinned in
`conformance/admission-checkpoints/prior.pin.json`. Validation checks actual binary
bytes and source inventory before execution. Old binaries create and execute the
histories; fresh new workers recover the same actual Temporal SQLite store and
replay real retained histories, including revoked authority and unknown effects.
No arbitrary future-version compatibility or power-loss guarantee is claimed.

## Reproduction and status

The complete candidate matrix is `tools/qualify_admission_checkpoints.py`:

```sh
PYTHONDONTWRITEBYTECODE=1 \
RUNSPINDLE_TETHER_V1_SOURCE=/path/to/legacy-tether-reference \
RUNSPINDLE_BUILDER_ARCHIVE=/path/to/exact-builder-closure.tar.gz \
/usr/bin/python3 tools/qualify_admission_checkpoints.py \
  --platform /path/to/pinned-platform \
  --platform-prepared /path/to/prepared-temporal \
  --composition /path/to/pinned-composition \
  --tether /path/to/pinned-tether \
  --prepared local/runtime-prepared \
  --prior-scoped /path/to/prior-scoped-source \
  --prior-scoped-prepared /path/to/prior-scoped-runtime \
  --prior-intent /path/to/prior-intent-source \
  --prior-intent-prepared /path/to/prior-intent-runtime \
  --report-dir /path/to/report
```

It runs the complete bridge regression aggregate, 22 checkpoint/revocation cases
twice, three specified-version upgrade integrations twice, and explicit failure and
SIGTERM cleanup probes. Inputs must be clean/pinned and remain unchanged. This is
an unqualified candidate until the final exact-source run and independent review
are recorded. Native peers and issuer identities remain synthetic; real provider,
native session and production activation remain outside this evidence.
