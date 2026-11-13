# Bounded Temporal development runtime

`runspindle.runtime.v1` semantic reference: composition commit
`4b700269be9a4feab7b955faf0229fbdd77d8aa9`, `specs/runtime-v1.md` and
`contracts/runtime-v1.json`. `Admission` JSON flattens the seven Binding fields;
strict Serde decoding rejects unknown fields, invalid integer encodings and input
above one MiB. JSON roundtrip/negative tests cover this native mapping separately
from the unchanged `runspindle.fixture.v1` test format.

## Durable ownership and bounds

`AdmissionRegistry` is one long-lived Temporal workflow per isolated namespace.
This tranche qualifies **one registry authority per namespace**; independently
started competing registries are unsupported. Registry identity must be selected
by the trusted composition boundary. Deterministic child IDs are namespace-global,
so a duplicate child collision fails closed; it is not cross-registry admission
reconciliation. No other process-local cache claims global admission uniqueness.

The registry retains at most 1024 accepted delivery keys and 1024 task/revision
bindings without eviction, expiry or continue-as-new. Known exact deliveries
reconcile at capacity; new delivery keys reject. Same-revision deliveries bind the
same digest/policy/operation/pin/epoch/grant. New revisions are consecutive,
explicitly reference their predecessor, and require its observed terminal result.
Operation IDs may not be reused by any new retained revision/task in this bounded
registry, a conservative extra conflict rule. Older duplicates retain their own job.

An update first calls the injected read-only verifier, then rechecks all conflicts
and commits its binding plus pending child dispatch in one workflow-state decision.
The SDK's recorded history replays that decision; no separate nondurable outbox is
used. One registry run serially starts/awaits deterministic child jobs, so an offline
or unknown operation holds later queued jobs. This throughput limit is intentional.
A lost start/update reply reconciles the original workflow ID; no new child ID is
invented. Atomicity/durability here comes from the real development Temporal server,
not the dependency-free legacy `AdmissionLedger` model.

## Operations and authority

`OperationWorkflow` records scheduled/acknowledged/unknown and actual observed
terminal states separately from requested/acknowledged/terminal cancellation.
An offline explicit host pin waits, never selects another host. Effectful launch
and cancellation activities use one attempt; failures/timeouts/lost replies lead
to read-only discovery/polling with the original complete binding. This subset
never relaunches, including when an absent receipt is observed. It does not expose
a `proven_absent` resubmission path. Native session ID comes from the scripted peer
receipt and cannot be synthesized from the job ID or rebound on reconnect.

Cancellation is an explicit update with the exact Binding, not a raw Temporal
workflow cancellation. It is rechecked after awaited authority/host observation
before scheduling launch. Pre-dispatch cancellation produces an internal terminal
`observed_cancelled` result with **no native session/effect**; this is local dispatch
cancellation, not an observed native process stop. Pre-dispatch denial similarly
produces an internal `observed_failed`. Once a native effect may exist, only bound
peer observations establish terminal completion/failure/cancellation. Cancellation
acknowledgement alone never terminates an operation. Completion can win its race.
Raw administrative Temporal cancel/terminate APIs are outside this adapter contract
and have no native-stop guarantee.

`Ports { script_directory: None }` denies all authority and effects. Only explicit
synthetic test injection enables the scripted ports. Its grant record binds the
entire Admission, including policy/operation/selected target and grant reference.
Each consequential port action rechecks current authorization independently, so
replay of a prior verification does not authorize a new external command. There
is no production actor authentication, identity provider or credential resolution.

The simplified `PortReply` maps binding/session/status for this scripted peer.
It does **not** implement native sequence/time freshness, authenticated observation,
browser epoch fencing, multiple-session discovery or host/core wire compatibility.
The separately implemented host adapter owns its own freshness tests. These gaps
remain blocked even though the internal durable workflow is exercised.

## Scripted peer, CLI and evidence

The development binary modes are `worker http://temporal:7233 [script-directory]`,
`client http://temporal:7233 [command-file]` and `replay history-file`. A client
reads JSON from the file or bounded stdin. It never loads environment profiles,
provider keys or Temporal credentials and rejects other service origins. Commands
are `start{registry_id}`, `admit{registry_id,admission}`, `registry{registry_id}`,
`operation{job_id}`, `cancel{job_id,binding}`, `history{workflow_id}`. Client and
worker containers use only the platform lane's internal DNS origin.

A synthetic `script.json` is an array of exact admission/grant records with
`authorized`, `online`, observed `native_session_id`, `launch_lost_reply`,
`crash_after_launch`, nullable `terminal` (`completed|failed|cancelled`) and
`cancel_acknowledged`. Optional `observe_barrier` defaults to false. For the cancellation-race test it
holds the Online reply until a bounded controller release, after an observed
persisted cancellation request. The peer writes a separate bound Online return
receipt after that release; absence of release fails within 20 seconds. No public operational endpoint is enabled.
Independent `calls.jsonl` records are fully encoded then appended under a
process-safe file lock and fsynced. The fake peer uses create-new, fsynced receipt
files to observe effects independently of workflow results. Those files model a
peer, not a production effect store. A second launch fails instead of returning
success. Process exit93 after receipt persistence tests lost acknowledgement.

`tools/test_temporal_runtime.py` qualifies real SDK/server development persistence:
fresh worker processes, hard server restart against the same database, concurrent
updates, default-deny/revoked authority, offline pins, conflicting revisions,
cancellation races, independent calls/effects, and same-version real history replay.
Replay is also required to leave the independent peer ledger unchanged. It does
not prove compatibility across SDK/workflow versions or production database failure.
Browser/delivery/provider ports are not exercised. Issues #1–3 remain open for
production authority, actual sibling protocols, trusted freshness/fencing, durable
accepted artifacts/delivery and wider store/concurrency/version qualification.
