# Bounded synthetic Temporal Browser workflow

This separate optional `artifact-store` workflow coordinates the real
[BrowserRegistry](BROWSER_EFFECT_REGISTRY.md) and wire client. It does not provide
a live Browser adapter, credential resolver, production issuer, native session
producer or deployment. The Browser peer used here is an independent synthetic
SQLite effect ledger. The separate pinned actual Browser receiver tests remain a
different evidence class; passing both lanes is not full-system live acceptance.

## Identity and authority

`start_browser` derives one workflow ID from principal, intent and revision.
Temporal duplicate-ID rejection and the registry's immutable identity are separate
barriers. A workflow contains only the exact target, not payload bytes or grants.
Only the explicitly selected development worker directory supplies a bounded
payload file and independent synthetic grant script. No directory means deny.
Each activity rechecks exact target, digest, action and expiry, including inside
the registry and before the independent effect. CLI input is not authority.

Initial dispatch activity retry cannot grant a second dispatch: the registry
commits its reservation before entering the peer. A crash after reservation but
before the site effect can leave Unknown permanently. A crash after the site
commit but before activity completion also recovers without resending.

The workflow never polls the site automatically. An exact-target reconcile update
queues a separately authorized observation; it never changes target or sends the
payload again. Acknowledgement and lost replies remain Unknown. Confirmed means
that this synthetic peer read back the exact independent effect. Query snapshots
are explicitly historical, not fresh authority or a transferable receipt.

## Bounds and operations

The distinct queue is `runspindle-browser-v1`. The workflow permits at most sixteen
explicit reconciliations and 120 idle one-second ticks, plus a ten-minute execution
timeout. Activities have bounded timeout/retries. Exhaustion ends coordination,
not the registry reservation. Completed workflow IDs cannot reset the budget.
Unknown is not permission to mint a replacement operation.

Development commands are `browser-worker`, `start_browser`, `browser` and
`reconcile_browser`. The sole optional worker configuration directory is `/effects`.
No service is installed or started implicitly by a build or test command.

## Verification

`tools/test_browser_temporal.py` uses a newly source-bound Core runtime and the
existing pinned Platform disposable internal-network Temporal lane. It exercises
real worker exits before/after independently committed effects, abrupt Temporal
server restart with retained development database, a fresh worker, revoked and
restored reconciliation authority, changed-target rejection, and real history
replay without peer calls. All resources belong to the bounded test and are
cleaned up. Results identify exact image/source hashes and preserve synthetic
boundaries; this is not storage-device power-loss or production rollback evidence.

Core #3 and Browser #8 remain open for actual site adapters, composition-level
wiring and full acceptance. Existing runtime receipts apply only to their recorded
source and image, not automatically to a later commit.

## Recorded result

The [normal](browser-temporal-tests.json) and
[optimized-Python](browser-temporal-tests-optimized.json) exact-source reports
passed both real crash
scenarios on image `sha256:6231556c13869d9e4d5d579d3b3233034f3af13bcdb68e2a013e44709bb9c274`.
After-effect recovery retained one site effect; before-effect recovery retained
zero and remained Unknown. Each scenario made one original dispatch peer call
and one separately authorized readback. Revoked reconciliation and actual
history replay added no peer calls. Running/completed duplicate-start rejection
and exact site-byte/target checks also passed. Six Rust component tests cover
default denial, grant changes/expiry, identity, acknowledgement uncertainty,
explicit reconciliation and request bounds.

`docs/runtime-build.json` now describes this freshly built executable; older
Temporal/admission reports remain evidence for their original images only. The
full prior 53-phase admission checkpoint was not rerun for this change.
