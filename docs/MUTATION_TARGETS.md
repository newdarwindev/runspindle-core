# Mutation targets in isolated partitions

Mutation jobs cannot assume a previous job's build directory exists. Each job
uses a private temporary target. If `local/linux-target` exists, its dependency
artifacts are copied; otherwise the private target starts empty. The existing
pinned Docker builder and locked offline Cargo command must compile the tests.
No downloads, fallback builders or success-on-missing-cache behavior are added.

Component outputs are cleaned before each mutant. The original target's presence,
file sizes and modification times must remain unchanged. Private outputs are
removed after success or failure. Runtime mutation source copies also use the
system temporary directory, without requiring an untracked `local` directory.

Synthetic Python tests cover absent/existing seeds, seed changes and cleanup.
They do not qualify the Rust mutation results. Runtime mutation detection still
requires the exact named test to fail behaviorally; compilation failures cannot
count as detection. Prepared builder architecture and dependency closure remain
separate prerequisites. A cold target can cost more compilation time and must
still fit the existing bounded timeout.
