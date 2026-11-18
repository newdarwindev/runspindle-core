# Actual Browser receiver through Temporal

The explicit `browser-worker-pinned-fixture` development mode composes the real
Temporal workflow, Core BrowserRegistry/client, and pinned Browser
`GuardedJournal`/wire receiver. It does **not** automate a real browser or website.
The site adapter and both authority sources remain synthetic. Core #3 and Browser
#8 remain open for actual site observations/input, production authority and wider
composition acceptance.

## Fixed, source-bound transport

The worker uses only `/effects`, `/browser-source`, `/browser-fixture` and
`/usr/bin/python3`. Workflow input cannot choose a helper, command, path, peer
mode or credential. Missing explicit configuration denies; there is no fallback
to a synthetic success. The previous `browser-worker` mode is unchanged.

The helper script and Browser source pin are embedded in the Rust executable.
Mounted helper files must match those exact bytes, not merely filenames. Python
runs with isolated imports, disabled bytecode writes, cleared inherited environment
and fixed arguments. The helper independently verifies the pinned Browser package
inventory and every runtime source hash before loading it. Unselected modules,
symlinks and standalone bytecode/extensions are rejected. The source remains a
read-only mount. The prepared runtime's source inventory includes both embedded
fixture inputs; earlier source trees without this transport keep their old
inventory rules.

The exchange has bounded concurrent input/output and a two-second deadline.
Owned child processes and I/O threads are killed/reaped/joined on errors before
returning a fixed sanitized failure. A test-only marker exits the Core worker only
after its child has been reaped. No persistent service, credential or external site endpoint is
configured by this mode.

## Independent expectations and effects

Core authority rereads its own exact target/digest/action/expiry script. Browser
receives independent `target.json` and `payload.bin` fixture expectations, never
an authority grant constructed from request fields. Its own revoked marker can
deny even when Core authority remains valid. Browser's actual guarded journal
retains reservation and observation state.

An independent SQLite site ledger records exact operation, target and bytes.
Submission deliberately loses the response. Separate receiver-exchange and
site-call ledgers distinguish entering Browser from invoking the site. A before-
site crash occurs after the Browser reservation and recorded submit attempt, but
before any site effect. Missing effects do not grant new dispatch rights.

## Reproduce and interpret

Run `tools/test_browser_temporal.py` with explicit prepared Core/Platform inputs
and `--browser-source` pointing to the exact pinned Browser package. This starts
only a bounded disposable internal-network Temporal development lane. Omitting
that option exercises the older scripted-peer lane instead.

The actual-receiver variant checks real worker exits before/after the site effect,
abrupt Temporal server restart, fresh worker recovery, Core revocation with zero
Browser entry, independent Browser revocation with zero site readback, explicit
authorized reconciliation, running/completed duplicate rejection and actual history
replay with zero receiver entry. Site effects remain synthetic: these results are
not real browser-engine, website, issuer, device-power-loss or live acceptance.
The outer harness can run under optimized Python; the fixed isolated receiver
interpreter remains Python 3.11.2 in normal mode.

## Recorded result

Both [normal](browser-through-temporal-tests.json) and
[optimized outer-harness](browser-through-temporal-tests-optimized.json) runs pass
on image `sha256:3e10dad5586bfc169c3da6296e63ae194f9fe64978322ef6ba65944d4e92a477`.
The Browser package is pinned at
[`b3edadf`](https://github.com/newdarwindev/runspindle-browser/commit/b3edadf12af74cd5b5d27a486ef0a4a64d5a1594).
Each crash scenario has exactly three receiver exchanges (original dispatch,
Browser-denied reconciliation, authorized reconciliation), but only two site calls
(original submit attempt and authorized readback). Core-denied reconciliation and
history replay add no receiver exchanges. After-effect recovery has one exact
site effect and Confirmed; before-effect recovery has zero effects and Unknown.
Both owned lanes clean up successfully. The embedded fixture/pin bytes are part
of the exact source inventory in these reports, not inferred from a commit name.
