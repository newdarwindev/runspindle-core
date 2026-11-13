# Real host library Temporal mapping (H02)

The `host-runtime` feature uses `runspindle.integration.v1`, composition contract
`96299842505ce7662178b7c7b00fd31f6c2754f0`. `Cargo.toml` and `Cargo.lock` pin the
reviewed host library merge `6caf08d3f642691ceecffc76c5aa378fbdd7a352` by Git revision.
Its SQLite store owns dispatch/cancel reservations, exact seven-field bindings,
native identity, sequence/time and receipt queue. The separate original
`AdmissionRegistry`/`OperationWorkflow` and `Ports` remain scripted regression APIs.

`HostAdmissionRegistry` reuses the bounded Runtime v1 admission decision model,
with an injected default-deny verification activity. It dispatches deterministic
child IDs to `NativeOperationWorkflow`. Every activity opens the actual pinned
host adapter and store; `host_binding` maps all seven fields without synthesizing
sessions. `Prepare` durably queues before possible dispatch, so cancellation while
queued yields the library's explicit local `CancelledBeforeDispatch`, with no
native terminal or native session. Only an actual native terminal observation
satisfies the unchanged Runtime v1 successor-revision gate. Rejection/local cancel
therefore does not silently fabricate a native terminal to release that gate.

Effects use one Temporal attempt; scheduled or uncertain host reservations cannot
launch again. Fresh workers call the same real adapter, which reconciles before
later effects. Unknown/ambiguous discovery returns the persisted Unknown snapshot;
invalid observations reject. Cancellation acknowledgement never becomes terminal
completion. Actual completion/failure racing cancellation remains the real result.
Host freshness uses explicit trusted adapter time and a 30-second maximum age;
wall-clock regressions fail closed. Production time/authority providers remain
unqualified. The durable receipt queue is retained, bounded at 4,096, and requires
explicit consumer acknowledgement/draining before capacity; this finite test lane
introduces no background consumer or retention policy.

`HostPorts {directory: None}` denies. The development directory supplies a bounded
exact `native-script.json` grant/request, online state and fault controls. This is
an injected synthetic authority and peer, never a production identity service.
The scripted peer has an independent `native-peer.sqlite` call/session ledger.
It records observations before a lost reply or process exit and treats a duplicate
launch as a ledger conflict. It is not a native Codex app-server implementation.
Capture, browser, provider and real destination/Tether remain blocked.

Development CLI adds `start_host`, `admit_host`, `host_registry`, `native` and
`cancel_native`; `admit_host` accepts the explicitly versioned HostRequest wrapping
an unchanged Runtime v1 Admission. Unknown versions and changed bindings deny.
The exact prepared runtime registers both workflow families for separate tests
and same-version replay. No upgrade/replay compatibility with prior histories is
claimed.

Core retains an accepted Temporal cancellation request independently of host
observation state. Queries project Requested while the host action is pending.
When a held actual terminal observation wins the race, core retains that outcome
and marks the accepted cancellation ObservedTerminal. This combines two observed
facts without fabricating a native cancellation effect or changing session IDs.
The controlled H02 terminal-reply barrier independently persists the host's
completion with no cancellation, accepts the Temporal cancel update, then checks
return-receipt and history order. No late peer cancel is dispatched.

## Additional admission recovery qualification

`tools/test_admission_runtime.py` exercises four concurrent authenticated (scripted exact-request authority) admission updates through the real HostAdmissionRegistry. It distinguishes delivery duplicates, existing revisions, authenticated conflicts and explicit successor gating, then replays registry and child histories with independent peer call counts. `authority_valid_until_ms` is an optional trusted synthetic grant expiry; it is checked at admission and every host action. The missing value retains the existing scripted authorization semantics; no production expiry/authentication claim follows. `crash_after_prepare` interrupts the synthetic development worker only after host queue commit and before its activity reply. Fresh worker/server recovery retains the same child and queues expired authority without launch. Admission replies are discarded/replayed; a precise server kill between the workflow admission mutation and child creation is not separately injected. See [evidence](../docs/admission-recovery-tests.json).
