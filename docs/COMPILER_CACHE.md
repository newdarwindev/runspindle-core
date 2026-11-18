# Prepared compiler dependencies (opt-in)

This is a local development optimization under qualification, not evidence of a
50% end-to-end CI speedup. No workflow enables it by default.

A seed is built offline in the exact selected pinned Linux x86_64 builder, from
fresh sources and an empty target. `cargo test --no-run` prepares compilation;
then `cargo clean -p runspindle-core` removes the component and its test outputs.
No test verdict is cached. Incremental state is removed. Cargo-generated internal
hardlinks are copied into independent sealed inodes, never exposed to jobs.

The identity includes source/tests, Cargo manifests/lock, toolchain pins, exact
builder image selection, feature/profile/architecture and cache/consumer source.
Jobs must match it exactly. They get private writable targets copied from hashed,
read-only dependency files. Each mutation still cleans the component, recompiles
its altered source and must fail its actual behavioral test, not compilation.
The existing uncached path remains available; wrong/poisoned caches fail closed.

## Measurement protocol

Compare identical source, image, feature set, CPU/memory limits and complete
mutant/test workload. Record whether the seed construction cost is included or
amortized. Run cold/warm pairs at least three times, keeping all output and
failures. A warm run must not skip baseline tests, mutants or integrity checks.

Local compiler/mutant timing is **not** end-to-end CI. The CI metric separately
starts at first VM reservation and ends after final aggregate and owned cleanup;
include boot, checkout, all mandatory partitions and receipt aggregation. Record
summed runner-active time as a secondary metric. Do not compare successful warm
runs against historical image-binding failures or count an input-stage
microbenchmark as the total result. The 50% goal remains unproven until a matched
successful CI baseline and warm repetition demonstrate it.

The key also binds the actual mounted Cargo registry/git bytes and timestamps.
Only the keyed source snapshot is mounted for preparation; unsupported Cargo
configuration/build-script paths are rejected. Cached mtimes are verified because
Cargo uses them in freshness decisions. Missing/symlink target roots and writable
manifests are rejected. Cache entries are owner-private, with independent
read-only files. Unexpected consumer termination or unconfirmed container cleanup
quarantines the private workspace instead of deleting possibly mounted data.
Such a run is not successful and requires explicit inspection/recovery.

Preparation and local measurement require an already verified native builder;
there is no download, emulation, daemon activation or runner registration fallback.
A prepared seed is not a saved backup or a successful-test receipt. Every consumer
reruns its own checks; existing behaviorally failing mutant assertions remain.

## Recorded local results

[Exact receipt](compiler-cache-benchmark.json), frozen source tree
`ab4144e993e5d46b487f2f522b4c8f96585779d7`:

| Complete four-mutant workload | Median elapsed |
| --- | ---: |
| Fresh target, cache disabled | 294.49 s |
| Verified dependency seed, private target | 59.88 s |

Three interleaved cold/warm repetitions passed all 24 behavioral mutant detections.
Four separate pristine positive controls also passed through the same cache path.
This is **79.67% less elapsed time for this local mutation workload**, including
key/dependency/seed validation, copying, compilation, assertions and cleanup.

Seed construction took 113.39 s and produced 2,527 dependency files (1.826 GB).
Including that one-time preparation across three runs gives 66.56% less total
elapsed time. For only one use, the measured median plus preparation gives 41.17%
less time, not 50%. These are application-target cache measurements, not cold OS
page-cache measurements. No successful test verdict or mutant output was reused.

**The requested 50% end-to-end CI target remains unverified.** Other partitions,
VM acquisition, checkout, aggregation and fleet retention are not included.
The host cache's reserve policy is not yet qualified for 24 GiB fleet guests;
exact guest builder/dependency agreement and workload admission remain required.
