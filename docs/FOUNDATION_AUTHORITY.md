# Foundation authority reconciliation

This is a stacked candidate over published core PR #21 (`0d9f23bc680b7e932cbfb903bd62d75764c498ca`)
and core hygiene PR #20. It does not make either prerequisite merge eligible.
The original authority worktree was preserved. Eight tracked dirty files were
privately snapshotted with HEAD, binary patch, status and SHA-256 hashes, applied
onto the published PR #21 head in a new worktree, and byte-verified before editing.
No private snapshot, key, raw history or configuration is published.

The implementation preserves the historical workflow registrations. The disabled
`authenticated-worker` entrypoint now rejects before parsing or connecting to a
Temporal target when the binary lacks `verified-authority`. A negative executable
test checks that boundary. Another negative test checks that a missing trusted
helper cannot use the script's `authorized` flag to invoke even an observation or
launch adapter. These implement the existing optional-feature and deny-only
contracts in `specs/verified-authority-v1.md` and scoped-effect intent in
`specs/desired-intent.md`.

The reconciled harness independently inspects Temporal child workflow IDs and a
separate fsynced scripted-peer call/effect ledger. It covers concurrent duplicates
and conflicting revisions, forged signatures and changed actor/audience/policy,
expiry and revocation while pending, unavailable/stale authoritative SQLite state,
a lost acknowledgement followed by abrupt worker/server restart, retained native
identity, cancellation scopes, and replay without additional effects. Assertions,
keys and signatures are checked absent from retained Temporal payloads. Cleanup
verifies that containers, the network and Temporal volume no longer exist.

Local observations use the pinned Linux aarch64 builder, Rust 1.94.1, Temporal SDK
1.2.0 and development CLI/server lane. The runtime image is
`sha256:202af35637b1e422382dbc3ca578d06915a813fc2725f3526ad1722cc86384b9`;
its executable SHA-256 is
`f1c4ff3d272f081a10f9bd42aeeeb96cf007c752897ac24e7bfe12fe4d0deff8`.
`docs/runtime-build.json` records exact runtime source/dependency hashes. Synthetic
issuer state and actual SQLite/Temporal stores are real; host/native/provider
behavior remains scripted or unexercised. This is not the required Linux x64 CI
qualification or system acceptance.

The current candidate's first real-store recovery observation passed with four
independently observed child IDs, three authorized launches, one cancel call and
zero forbidden launches or retries. Repeated and final aggregate results belong
to the exact-source review packet; this document does not substitute a prior run
for a changed-head check. An earlier aggregate failed on Python 3.9 annotation
evaluation; postponed annotations fixed that failure. Historical reports remain
historical. No failed or unavailable gate is converted to success.

The hygiene baseline is unchanged. New diagnostics are fixed with typed harness
interfaces and formatting. Mandatory checks include the authority feature and all
implemented runtime/artifact/outbox/authority mutation checkers. They require the
pinned builder and cached dependencies; absent tools must fail, never skip.
Qualification compares source fingerprints before/after checks, including core,
composition and platform tooling. This detects persistent source drift, not a
malicious change restored between observations.

## Core issue #16 criterion ledger

| Criterion | Candidate evidence | Remaining boundary |
| --- | --- | --- |
| Trusted actor, scope, intent/policy, expiry | Ed25519 and local authoritative SQLite verification; component and real-store negative observations | Production issuer, remote identity/clock trust unqualified |
| Exact destination | Host/workspace/lease target and entire canonical request digest verified | **Blocked:** admission envelope has no delivery account/destination binding; delivery wire conformance grants no authority |
| Forged/expired/conflicting evidence creates no unauthorized child | Actual registry history child IDs plus separate adapter/effect ledger | Synthetic caller and host only |
| Dispatch expiry/revocation, concurrent conflicts, replay, unavailable trust | Real Temporal/SQLite pending queue and fresh-process recovery; deny-only helper test | Repeat/exact-candidate aggregate must pass before claiming qualification |
| Deliberate mutation | Wire sequence and dispatch guard mutations fail behavioral assertions | Required x64 CI remains separate |
| Source/config/dependency/toolchain pins and reproducible command | Build manifest plus bounded qualifier below | Hygiene/envelope merges and exact-head independent review required |
| Parent issue #2 | Preserved historical models and replay entrypoints | Broader native/browser/delivery/provider/system acceptance remains open |

Core #16 and parent #2 remain open. Completing host/workspace offline authority is
insufficient for the exact-destination criterion. A separately versioned admission
contract and enforcement across delivery will be needed; this candidate does not
invent an unreviewed destination field or widen the approved authority scope.

Run the documented preparation in `VERIFIED_AUTHORITY.md`, then use:

```
python3 tools/qualify_verified_authority.py --platform /path/to/pinned/platform --platform-prepared /path/to/temporal-prepared --composition /path/to/envelope-composition --report-dir /path/to/private/reports
```

Reports stay outside tracked source. Full integration, production activation,
provider/account credentials and live acceptance are not run.
