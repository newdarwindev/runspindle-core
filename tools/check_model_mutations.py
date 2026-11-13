#!/usr/bin/env python3
"""Verify three deliberate model defects fail tests in disposable offline copies."""
import json
from pathlib import Path
import shutil
import subprocess
import tempfile

ROOT = Path(__file__).resolve().parents[1]
MUTATIONS = [
    ("uncertain_becomes_retryable", "src/artifact.rs",
     "DeliveryDecision::ReconcileRequired", "DeliveryDecision::Started",
     "every_artifact_action_trace_through_length_seven_matches_transition_table"),
    ("id_accepts_129_bytes", "src/identity.rs", "value.len() > 128", "value.len() > 129",
     "id_alphabet_and_length_boundaries_are_exhaustive_for_ascii"),
    ("conflict_consumes_intent_revision", "src/identity.rs",
     "Err(AdmissionError::DeliveryConflict)",
     "{ self.intents.insert(intent.key.clone(), intent); Err(AdmissionError::DeliveryConflict) }",
     "failed_delivery_reuse_does_not_register_a_new_task_or_revision"),
]


def execute(directory, test=None):
    command = ["cargo", "test", "--offline", "--test", "sequence_contracts"]
    if test:
        command.extend([test, "--", "--exact"])
    return subprocess.run(command, cwd=directory, capture_output=True, text=True,
                          timeout=60)


def main():
    results = []
    with tempfile.TemporaryDirectory(prefix="runspindle-core-mutations-") as temporary:
        work = Path(temporary)
        for name in ["Cargo.toml", "Cargo.lock"]:
            if (ROOT / name).exists():
                shutil.copy2(ROOT / name, work / name)
        for name in ["src", "tests"]:
            shutil.copytree(ROOT / name, work / name)
        baseline = execute(work)
        if baseline.returncode != 0:
            raise SystemExit("Baseline must pass before mutation checks:\n" + baseline.stdout + baseline.stderr)
        for name, path, before, after, test in MUTATIONS:
            target = work / path
            original = target.read_text()
            if original.count(before) != 1:
                raise SystemExit("Mutation anchor must match exactly once: " + name)
            try:
                target.write_text(original.replace(before, after))
                result = execute(work, test)
                # Compiler failures, zero selected tests or runner errors do not
                # count as a detected invariant. Require a named failed test.
                detected = (result.returncode == 101
                            and ("test " + test + " ... FAILED") in result.stdout
                            and "1 failed" in result.stdout)
                if not detected:
                    raise SystemExit("Undetected mutation " + name + ":\n" + result.stdout + result.stderr)
                results.append({"mutation": name, "test": test, "outcome": "detected"})
            finally:
                target.write_text(original)
    print(json.dumps({"status": "passed", "evidence_class": "local_synthetic_mutation_tests",
                      "mutations": results, "temporary_copy_cleanup": "completed",
                      "product_acceptance": "not_run"}, sort_keys=True))


if __name__ == "__main__":
    main()
