# Native Temporal through sandboxed Chromium

This explicitly selected development fixture extends the pinned Browser receiver
lane to real Chromium renderer execution against an owned synthetic HTTP website.
It does not qualify production account/TLS/frame authority, credentials, profile
custody, external services or live H05 acceptance. Core #3 and Browser #8 remain
open. Existing synthetic and pinned-library modes are unchanged.

## Trusted wiring

`browser-worker-chromium-fixture` selects embedded helper bytes and a separate
Browser source manifest. Workflow input cannot select a helper, executable,
source path, destination URL or credential. Fixed mounts `/effects`,
`/browser-source` and `/browser-fixture` are supplied by the trusted harness.
The helper verifies both Python package inventories and the CDP helper bytes,
ignores bytecode caches, and creates no unverified `tools` import search path.

The actual path is Temporal Rust SDK → durable Core BrowserRegistry → bounded
Python peer → Browser Receiver → GuardedJournal → sandboxed Chromium renderer →
synthetic loopback website. Core's script authority and Browser's injected guard
are distinct. `transport.sqlite` records receiver exchanges and helper site calls;
`site.sqlite` independently records the actual HTTP site's binding, payload
digest and outcome. It does not retain raw submitted bytes.

Unknown reservation-before-peer results do not permit blind resubmission.
Reconciliation requires fresh authorization and independent site evidence.
The engine uses a distinct versioned workflow family, queue and workflow-ID
prefix with an eight-second helper deadline and deterministic ten-second
activity timeout. Existing BrowserWorkflow keeps its two-/three-second limits.
Slow startup fails closed; this is not a general production browser latency guarantee. No worker-local
configuration changes workflow timeout commands or replay behavior.

## Separate native qualification

The native lane is deliberately separate from the existing Docker runtime lane.
It validates the prepared Core source/binary/image binding first, then verifies
the **same native executable bytes** before running them with host libraries.
The Temporal CLI executable is pinned to SHA256
`6968c86c97d91d4b49e19c0c9b0842bfb5eb2d356f22b989006d1e09164eab5b`.
Native runtime hashes are recorded, not mislabeled as Docker image acceptance
or complete Chromium shared-library closure.

The outer verifier needs local Docker access to inspect the build artifact. Each
scenario drops to an unprivileged user and uses a fresh Bubblewrap network and
PID namespace, minimal temporary root, read-only executable/source mounts, and
one owned writable fixture directory. Only loopback is present. Temporal binds
127.0.0.1; a namespace-local hosts file maps the existing allowed name `temporal`
to that loopback address, without changing the host hosts file. No ports or
routes are exposed to the host or Internet. The PID namespace
and parent-death handling bound descendants on interruption. Chromium retains
its sandbox and its own parent-death supervisor. No host root directories,
credentials, persistent service, provider calls or quota are required.

Run with explicit prepared directories and the pinned Browser source checkout:

```sh
sudo -n python3 tools/test_browser_chromium_temporal.py \
  --core-prepared /path/to/new/core-prepared \
  --temporal-prepared /path/to/temporal-prepared \
  --browser-source /path/to/pinned/browser \
  --user standard --report docs/browser-chromium-temporal-tests.json
```

The harness covers before/after-site worker interruption, real Temporal SQLite
server restart, fresh Core worker, no duplicate send, changed target refusal,
independent Core and Browser denial, explicit authorized readback, completed
workflow duplicate rejection and actual history replay with zero peer calls.
Both normal and optimized outer Python modes run; the fixed embedded peer uses
`python3 -I -B` and stays normal mode, as recorded separately. Historical older
admission/image reports are not evidence for this new binary.


## Verified result

[Native receipt](browser-chromium-temporal-tests.json) records four passing cases:
before/after-effect interruption in each normal/optimized outer Python mode.
Each case makes exactly three receiver exchanges and two site calls. After-effect
recovery confirms one independent site event; before-effect recovery retains
unknown with zero events. Actual history replay adds no receiver/site calls.
All recorded inputs remained unchanged across the run.

The [existing pinned-library workflow regression](browser-library-after-engine.json)
also passed both before/after cases on the same new artifact. The original
Browser workflow and its activity-options source remain byte-identical to the
previous commit; the engine family has a separate queue and workflow ID.

Full pinned Rust tests and strict Clippy passed. Both default Python modes passed
118 tests, with three opt-in skips; the actual namespace cleanup opt-ins were
separately exercised in both modes (2/2 including output overflow), as was the
existing pinned Browser source-fence test (1/1). The Tether
opt-in remained skipped in this default run, not relabeled as engine evidence.
Aggregate static quality passed with zero new diagnostics and 286 recorded debt
entries; no baseline change. Independent source/harness/receipt review verified
the native binary binding and 67 recorded Core/Browser/host input hashes.

The earlier two-second helper proved unsuitable for engine startup. The distinct
engine workflow corrects that limit without changing old workflow histories.
These results neither close the parent issues nor rerun every historical
admission/storage/provider qualification against the new executable.

## F07 bounded-source refresh

The engine source manifest now pins Browser revision
`7f3ab01cced0a96a091a22d42886486bf8600d4b` (Browser PR #55), including its
1,024-operation capacity and retained/schema-field bounds. The original
pinned-library fixture remains intentionally unchanged for compatibility tests.
The [refreshed native receipt](browser-capacity-temporal-tests.json) qualifies
the new engine pin rather than reusing the previous artifact's receipt. Its
before/after scenarios complement Browser's saturation, concurrent last-slot and
capacity-limit mutation acceptance; they do not claim a production storage quota.
The older library regression receipt is historical evidence for its recorded
artifact, not a test of this newer binary.
