# Owned child Browser peer (candidate)

The opt-in Linux `browser-child-peer` feature transports the existing
`runspindle.browser-effect.v1` request over anonymous inherited pipes. It adds no
service, network listener, issuer, account store, or effect ledger. The existing
Core `BrowserRegistry` remains the reservation/reconciliation owner. Autosave
credentials and version markers are a separate protocol, not implicitly mapped.

A trusted application explicitly supplies an absolute program, bounded arguments
and a 10 ms–30 s total exchange deadline. It must verify executable/source identity
and compose the Browser receiver with an independently authorized out-of-band
ticket. Wire fields and request hashes are not authority. There is no default
executable or permissive production CLI. Arguments must not contain secrets.

The child starts with an empty environment, private process group, piped stdin and
stdout, and discarded stderr. Each direction contains exactly one unsigned
32-bit big-endian length followed by the JSON bytes and EOF. Request/response
limits are the existing codec limits; response length is at most 4096 bytes. Core
closes stdin after the request; complete response, EOF and successful child exit
are required. Nonblocking parent pipe endpoints avoid a stalled reader/writer
escaping the exchange deadline. Failure produces only a fixed error, which the
existing wire client treats as unknown, never permission to resubmit.

Cleanup signals the owned process group before reaping the leader, including on
failure. A non-consuming wait observes completion so PID reuse cannot turn cleanup
into a signal to a reused leader. The deadline is a stop trigger, not a guarantee
against kernel-uninterruptible tasks. This is a trusted child lifecycle boundary,
not a sandbox against a hostile process escaping its process group or modifying
application state. It does not authenticate peers or install a runtime authority.

The safe pinned `rustix` APIs preserve the crate's `forbid(unsafe_code)` rule;
no host security, networking, trust-store or permissions settings are changed.

Local framing/deadline tests are component tests with explicit synthetic Python
children. An opt-in integration test pins Browser source `379f020c650f3571867a17c6e84f28ff953d6770` and exercises the actual Python Receiver/GuardedPeer over OS pipes: a synthetic target effect occurs, its reply is lost, the Core registry reopens, duplicate dispatch remains unknown without another effect, and a fresh child reconciles. The test uses a target-owned fixture file, not Browser persistence. This proves registry reopen in the same Rust process and fresh Python children, not Rust crash-restart qualification. No live website, credentials, production IPC, CI runner admission or
complete Browser parity is claimed.
