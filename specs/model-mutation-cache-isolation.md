# Disposable model mutation build outputs

`tools/check_model_mutations.py` assigns each temporary source copy its own
`target` directory. It overrides inherited `CARGO_TARGET_DIR` only for its child
Cargo invocations; the parent environment and unrelated build output are untouched.
The baseline and three deliberate mutations share that one disposable target,
which is removed with the source copy on success or failure. Offline operation,
60-second command deadlines and named failing-test requirements are unchanged.

This prevents timestamp-preserving source copies from reusing a previous run's
mutant executable in a new baseline, or contaminating another validation's cache.
It is compiler-output isolation, not an OS/network sandbox or product qualification.

Regression tests inspect the actual subprocess invocation for distinct copies,
with/without an inherited target, and verify an unrelated output sentinel remains
unchanged. Run `python3 -m unittest discover -s tests -p test_model_mutation_cache.py`
and `python3 tools/check_model_mutations.py`. Repeat the latter with an inherited
shared target to verify each run starts from a passing unmodified baseline and
still detects all three behavioral mutations.
