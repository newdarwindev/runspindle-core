# Prepared H02 development verification

Dependencies and source/binary/image bytes are pinned in Cargo.lock,
`docs/runtime-dependencies.json`, `temporal/toolchain.json` and
`docs/runtime-build.json`. Preparation verifies actual cached host source bytes
against its exact Git revision and bundled SQLite bytes against the inventory.
The builder and all tests use immutable image IDs, no egress, and cached crates/Git.

```sh
python3 tools/pin_runtime_dependencies.py
python3 tools/prepare_temporal_runtime.py --acquire --features host-runtime
# Review/copy the resulting manifest to docs/runtime-build.json before execution.
python3 tools/offline_rust.py test --features host-runtime
python3 tools/offline_rust.py clippy --features host-runtime
python3 tools/test_host_runtime.py --platform /path/to/pinned-platform \
  --platform-prepared /path/to/pinned-platform/local/temporal-prepared
python3 tools/test_temporal_runtime.py --platform /path/to/pinned-platform \
  --platform-prepared /path/to/pinned-platform/local/temporal-prepared
python3 tools/check_runtime_mutations.py
python3 tools/check_scaffold.py
```

H02 runs twice with the same seed and compares normalized outcomes. Real Temporal
admission, concurrent duplicate updates, offline pinning, process interruption
before/after the scripted peer effect, real server restart, session/sequence
recovery, wrong-epoch cancellation, completion racing cancellation, pre-dispatch
cancellation across restart and history replay have independent ledger assertions.
Zero peer receipts at the before-effect crash boundary is independently queried;
no retry is inferred from absence. Separate Rust tests reject future/stale/rebound/
ambiguous observations and default-deny/changed authority. Existing scripted-core
Temporal tests remain a separate suite. Native/core/host library integration is
bounded development evidence with a scripted peer; native wire, production and
full-system/live acceptance remain blocked.
