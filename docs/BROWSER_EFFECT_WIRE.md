# Core client for the Browser effect wire receiver

Partial [Core #3](https://github.com/newdarwindev/runspindle-core/issues/3) and
[Browser #8](https://github.com/newdarwindev/runspindle-browser/issues/8).
[Browser PR #52](https://github.com/newdarwindev/runspindle-browser/pull/52) supplies
the existing guarded-journal receiver. This change adds a real Rust encoder,
response verifier and trusted peer/authority interfaces, not a second journal.

## Client contract

`browser_effect_wire::Client` is enabled with `artifact-store`. Construction owns
an immutable copy of the entire Browser Target and nonempty payload (at most
1 MiB), computes the actual SHA-256 and rejects invalid identity/origin/bounds.
It emits `runspindle.browser-effect.v1` with exact target, digest and lowercase
payload hex; reconciliation sends null payload and requires separately checked
action authority. Its HTTPS origin validator intentionally accepts a conservative
subset without normalization, paths, credentials or whitespace. Exact bytes, not
normalized URLs, bind requests.

`Authority::verify` is a trusted current Core check before and after each peer
exchange. Default `Deny` permits no calls. Browser separately authenticates its
own one-shot ticket and keeps its actual guard; Core fields/hash are not tickets.
A post-exchange denial cannot undo a possible site effect and must not authorize
retry. Peer error or malformed/unbound response yields `Unknown`, not success.
No upstream error text is retained. Closed response decoding rejects duplicate
fields, unsupported versions/classes/outcomes, oversize data and a hash different
from the exact raw request. Confirmed/rejected are only scoped observations from
the configured trusted peer, never provider receipts, credentials or permissions.

The standalone client contains **no durable Core registry**. The optional
[BrowserRegistry](BROWSER_EFFECT_REGISTRY.md) now supplies durable identity and
single-dispatch reservation above it. Direct client callers must retain/reconstruct
the same original operation, target and payload across restart. They must not
invent another operation ID to retry unknown. The pinned Browser Journal owns
its durable reservation and no-resend rule; request hashes do not authenticate
peers. `Peer` implementations must supply bounded framing/deadlines, authenticated
peer selection and out-of-band tickets. This library installs no transport,
launcher, process, endpoint, browser/site adapter or issuer.

## Actual cross-language verification

`tests/browser_effect_integration.rs` explicitly opts into the actual Browser
package at commit `b3edadf12af74cd5b5d27a486ef0a4a64d5a1594` through
`RUNSPINDLE_BROWSER_WIRE_SOURCE`. `tests/fixtures/browser_source_pin.json` pins
all runtime Python module bytes. The fixture rejects changed, extra or symlinked
modules and untracked native extensions/standalone bytecode before import. It loads only that verified package, not arbitrary modules
from the checkout root, and does not consume its ignored bytecode caches.

The test-only peer launches a fresh bounded Python receiver for each exchange,
uses the actual GuardedJournal/SQLite code, and supplies independent synthetic
target/payload/action expectations. A separate SQLite site ledger records exact
bytes and submit/readback counts. No socket access is permitted. The tests cover:

- Site accepts then loses its response; Core also discards the bridge reply.
- Reconstructed Core client plus fresh Browser process keeps exactly one submit.
- Unresolved readback stays unknown; exact independent readback confirms.
- Revoked Browser authority cannot return even a stored confirmation.
- Wrong account, epoch, payload or separately admitted action causes zero effects.

This is Core client reconstruction **within the Rust test process** and fresh
Browser processes, not a Core process-crash or Temporal workflow qualification.
The two original client tests and the separate registry integration test are
opt-in and ignored in the default suite; run them explicitly with
`--include-ignored` and the pinned read-only Browser source. They never substitute
for missing live adapters. The literal Browser request/response vector is also
checked independently in Rust unit tests. Source-fence negatives and responsehash/
post-exchange-authority mutants require behavioral failures against clean baselines.

## Remaining work

The separate [BrowserRegistry](BROWSER_EFFECT_REGISTRY.md) now retains Core
identity and exact payload across process exits. The [synthetic Temporal Browser workflow](BROWSER_TEMPORAL.md) coordinates it
with a scripted peer. Production Browser/site-specific observation and input
adapters, issuer custody, distributed fencing and full F03/F07 acceptance remain
open. Earlier runtime-image/Temporal receipts keep their original source pins;
this client-level verification does not relabel those images as rebuilt.
