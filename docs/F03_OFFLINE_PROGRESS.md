# F03: pinned execution and uncertain-effect recovery

[Core #3](https://github.com/newdarwindev/runspindle-core/issues/3) remains open.
The original functional case includes both native launch and website submission
recovery. Native-only tests must not be presented as completing the browser slice.

| Original criterion | Current bounded evidence | Remaining boundary |
| --- | --- | --- |
| Offline selection executes nowhere else; lost launch acknowledgement consults existing session | Actual local Temporal H02 tests retain explicit host/workspace, original native session and one peer launch across worker/server reconstruction. | Scripted native peer is not live native authority. |
| Retry cannot duplicate uncertain submission | Native HostPorts and delivery outbox preserve unknown and reconcile rather than blindly launch/send again. The [Browser wire client](BROWSER_EFFECT_WIRE.md) now exercises the actual guarded Browser journal with a synthetic independent site ledger. | The wire client has no durable Core/Temporal Browser operation workflow or actual website adapter. Browser F07/F08 and full H05/H06 remain unqualified. |
| Concurrent stale actor is fenced | `concurrent_stale_actor_never_inherits_current_launch_or_cancel` releases current epoch 2 and stale epoch 1 HostPorts actors concurrently for dispatch and cancellation. Independent SQLite peer ledger remains one launch and one cancellation; stale requests fail, reconciliation retains observed session and epoch. | This is actual Core/Host libraries and SQLite with a synthetic peer, not distributed production fencing. |
| Cancellation requested/acknowledged/terminal remain distinct | Existing H02/Temporal tests exercise acknowledgement without terminal inference, completion winning the race, and pre-dispatch cancellation. | No browser/native live stop guarantee. |
| Restart cannot restore expired authority | Actual admission checkpoint and H02 fresh-worker/server tests exercise expiration and no forbidden effect. | Production issuer custody and power-loss qualification remain separate. |
| Deliberate behavioral mutations | Registry mutations remove capacity, conflict and operation-reuse guards. The targeted stale-actor mutant rewrites the caller request to the current authorized request before validation, and must fail the named concurrent regression. | This tests a representative authority-confusion defect, not every possible interleaving. |

The concurrent test uses bounded start channels, separate HostPorts invocations
and the actual Host SQLite adapter. It does not claim a forced lock interleaving:
whichever actor runs first, a stale caller must never inherit current authority.

Run the pinned offline builder with `host-runtime` (or the broader
`verified-authority,authenticated-artifacts` features), test target `host_runtime`,
then `tools/check_runtime_mutations.py`. Keep mutation runs serial with ordinary
builds: the mutation harness rejects any qualification-target inventory change.
Exact final-tree receipts record which checks actually ran. No provider/native
quota, external website submission or production activation is used.
