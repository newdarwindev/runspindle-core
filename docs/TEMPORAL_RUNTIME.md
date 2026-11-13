# Local Temporal development checks

Read [runtime semantics and limits](../specs/runtime.md) before running. All inputs
are synthetic. Public `activate()` and operational acceptance gates remain blocked.
There are no provider, browser, native app-server, delivery or production calls.

Pinned SDK 1.2.0 source is
[`c675c32469af707e1c979a7b808c70b4a0608b82`](https://github.com/temporalio/sdk-rust/releases/tag/v1.2.0).
Cargo.lock records every downloaded crate checksum; the SDK crate's own VCS
metadata resolves to that source revision. [Dependency inventory](runtime-dependencies.json)
records licenses/notice hashes before execution; preparation copies notices into
the local runtime image. [Toolchain pins](../temporal/toolchain.json) include the
Rust1.94.1 base image, exact Linux arm64 builder image, recipe/protoc/Clippy hashes and
package version. Exact Clippy1.94.1 acquisition is recorded in the dependency
inventory; feature-enabled offline Clippy runs in the qualified builder. Native macOS runtime build was unavailable because host `protoc`
is absent; actual SDK tests use the pinned Linux container. Other engine targets
or mismatching preparation artifacts fail closed without emulation/fallback.

Prepare dependencies explicitly (network acquisition is allowed only here):

```sh
python3 tools/prepare_temporal_runtime.py --acquire
```

The reviewed exact builder is a local prepared artifact, not a published image.
If absent, the pinned recipe can acquire a candidate, but any different image ID
blocks qualification and requires a separately reviewed toolchain pin. It never
silently substitutes a floating builder tag. The current lane is qualified on
Linux arm64 Docker29.9.2; this is not a universal installation claim.

Subsequent builds use cached crates with `--locked --offline` and Docker
`--network none`. Source hashes must remain unchanged while building. Preparation
writes ignored `local/runtime-prepared/manifest.json`; the reviewed
[runtime build record](runtime-build.json) pins source bytes, actual executable
SHA256, dependency inventory and immutable image ID. `tools/runtime_artifact.py`
checks that record, source hashes and image labels, creates a nonrunning/no-network
container to extract/hash the actual binary, and verifies cleanup **before** tests.
Missing/mismatching evidence is non-success; mutable manifest claims alone do not
qualify an image. A new source/image requires new reviewed build evidence.

Prepare platform's CLI1.10.0 lane separately using exact merged platform commit
`96eb567ef32c68c52c19ea8fd77f11e5340e291b`, then run:

```sh
python3 tools/test_temporal_runtime.py \
  --platform /path/to/merged-platform \
  --platform-prepared /path/to/platform/local/temporal-prepared
```

The platform context owns an internal Docker network with isolated gateway mode,
no published ports/UI, a disposable database volume, all workers/clients, readiness
and process deadlines and cleanup on failure/cancellation. Prepared tests perform
no downloads. Real Temporal workflow/history calls, not a process-start probe,
produce the recovery observations. Same-version replay happens against histories
fetched from that real database and invokes no activity ports.

Language checks (inside the exact prepared builder, no egress):

```sh
cargo test --locked --offline --features temporal-runtime
cargo clippy --locked --offline --features temporal-runtime --all-targets -- -D warnings
cargo fmt --all -- --check
python3 tools/check_runtime_mutations.py
python3 -m unittest discover -s tests -p 'test_*.py'
python3 tools/check_scaffold.py
```

The first two commands need the prepared Linux container/registry cache and target
directory as shown in `tools/prepare_temporal_runtime.py`; they are not asserted as
native macOS checks. The mutation script disables CLI linking in throwaway source
copies solely for registry-component tests; it must observe assertion failures,
not compilation failures. It cannot mutate the real workspace. GitHub CI and
live acceptance remain unverified/not-run; optional absent CI does not establish
runner capacity, deployment or full system acceptance.
