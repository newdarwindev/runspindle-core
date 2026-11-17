#!/usr/bin/env python3
"""Require a clean baseline before behavioral artifact-evidence mutant failures.

Execute only inside the already-approved isolated runner. No fallback, pull or
qualification-target writes. This script was authored, not run, for this change.
"""

from __future__ import annotations

import json
import pathlib
import shutil
import subprocess
import sys
import tempfile
import uuid

ROOT = pathlib.Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
from tools.mutation_target import isolated_target  # noqa: E402
from tools.prepared_builder import select  # noqa: E402

# Match the normal Rust validation feature set and existing offline lockfile.
# Literal canonical/signature vectors are independent of the Rust serializer.
FEATURES = "verified-authority,authenticated-artifacts"
mutations = [
    (
        "forged-validator",
        "forged_same_revision_validator_and_producer_signatures_are_rejected",
        {
            "verify_key(\n        &state.validator,\n        &validator.signer,\n        Role::Validator,\n        &validator.scope.validator.principal,\n        now,\n        &validator.signing_bytes()?,\n        &evidence.validator.signature,\n    )?;": "",
        },
    ),
    (
        "forged-producer",
        "forged_same_revision_validator_and_producer_signatures_are_rejected",
        {
            "verify_key(\n        &state.producer,\n        &producer.signer,\n        Role::Producer,\n        &producer.scope.producer.principal,\n        now,\n        &producer.signing_bytes()?,\n        &evidence.producer.signature,\n    )?;": "",
        },
    ),
    (
        "native-scope",
        "signed_native_identity_policy_principal_and_epoch_need_exact_authority",
        {
            "state.scope == producer.scope": "true",
        },
    ),
    (
        "commit-authority",
        "revocation_at_final_commit_check_rolls_back_all_new_rows",
        {
            "verify(evidence, trust, clock)?; // denial rolls back all three new rows": "// deliberately omitted final authority check",
        },
    ),
    (
        "byte-length",
        "byte_replacement_length_mismatch_and_failed_validation_never_accept",
        {
            "bytes.len() as u64 == scope.byte_length": "true",
        },
    ),
    (
        "retained-validation",
        "duplicate_acceptance_denies_missing_or_corrupt_validation_without_repair",
        {
            'anyhow::ensure!(retained_validation.is_some(), "retained validation missing");': "// deliberately omitted retained validation guard",
        },
    ),
    (
        "canonical-order",
        "independent_literal_canonical_and_signature_vector",
        {
            "entries.sort_unstable_by(|a, b| a.0.cmp(b.0));": "entries.sort_unstable_by(|a, b| b.0.cmp(a.0));",
        },
    ),
]


def container_run(
    root: pathlib.Path,
    target: pathlib.Path,
    builder: dict[str, str],
    command: list[str],
    timeout: int,
) -> subprocess.CompletedProcess[str]:
    # A CLI timeout does not stop its container. Retain an unguessable owned name
    # so finally can force-remove precisely this invocation, then verify absence.
    name = "core-artifact-evidence-" + uuid.uuid4().hex
    prefix = [
        "docker",
        "run",
        "--name",
        name,
        "--pull",
        "never",
        "--network",
        "none",
        "-v",
        f"{root}:/source:ro",
        "-v",
        f"{target}:/work/local/linux-target",
        "-v",
        f"{pathlib.Path.home()}/.cargo/registry:/usr/local/cargo/registry:ro",
        "-v",
        f"{pathlib.Path.home()}/.cargo/git:/usr/local/cargo/git:ro",
        "-w",
        "/source",
        builder["builder_image_id"],
    ]
    try:
        return subprocess.run(
            prefix + command,
            text=True,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            timeout=timeout,
        )
    finally:
        # No broad pruning, shared-container changes or daemon reconfiguration.
        # Failure to confirm cleanup aborts the harness, never counts a mutant.
        subprocess.run(
            ["docker", "rm", "--force", name],
            capture_output=True,
            text=True,
            timeout=15,
            check=False,
        )
        remaining = subprocess.run(
            [
                "docker",
                "ps",
                "--all",
                "--filter",
                f"name=^/{name}$",
                "--format",
                "{{.Names}}",
            ],
            capture_output=True,
            text=True,
            timeout=15,
            check=True,
        )
        if remaining.stdout.strip():
            raise AssertionError("owned mutation container survived cleanup")


def run_case(
    target: pathlib.Path,
    builder: dict[str, str],
    source: str,
    case: str | None,
) -> subprocess.CompletedProcess[str]:
    with tempfile.TemporaryDirectory(
        prefix="core-artifact-evidence-source-"
    ) as directory:
        root = pathlib.Path(directory)
        for item in ("Cargo.toml", "Cargo.lock", "src", "tests", "fixtures"):
            original = ROOT / item
            destination = root / item
            if original.is_dir():
                shutil.copytree(original, destination)
            else:
                shutil.copyfile(original, destination)
        (root / "src/authenticated_artifacts.rs").write_text(source)
        clean = container_run(
            root,
            target,
            builder,
            [
                "cargo",
                "clean",
                "-p",
                "runspindle-core",
                "--target-dir",
                "/work/local/linux-target",
            ],
            60,
        )
        clean.check_returncode()
        command = [
            "cargo",
            "test",
            "--locked",
            "--offline",
            "--features",
            FEATURES,
            "--target-dir",
            "/work/local/linux-target",
            "--test",
            "authenticated_artifacts",
        ]
        if case is not None:
            command += [case, "--", "--exact", "--test-threads=1"]
        else:
            command += ["--", "--test-threads=1"]
        return container_run(root, target, builder, command, 300)


def main() -> None:
    source = (ROOT / "src/authenticated_artifacts.rs").read_text()
    pins = json.loads((ROOT / "temporal/toolchain.json").read_text())
    _, builder = select(pins)
    for name, _, replacements in mutations:
        for old in replacements:
            if source.count(old) != 1:
                raise AssertionError("mutation anchor drift: " + name)
    cases = sorted({case for _, case, _ in mutations})
    observed = []
    with isolated_target(ROOT, "artifact-evidence") as target:
        # Run the unmodified suite first in the same image, offline closure,
        # feature set, test binary and private target used by every mutant.
        baseline = run_case(target, builder, source, None)
        if baseline.returncode or any(
            f"test {case} ... ok" not in baseline.stdout for case in cases
        ):
            raise AssertionError("clean baseline failed: " + baseline.stdout[-5000:])
        for name, case, replacements in mutations:
            mutated = source
            for old, new in replacements.items():
                mutated = mutated.replace(old, new)
            run = run_case(target, builder, mutated, case)
            if not (
                run.returncode
                and f"test {case} ... FAILED" in run.stdout
                and "could not compile" not in run.stdout
            ):
                raise AssertionError(
                    "mutant not behaviorally detected: " + run.stdout[-5000:]
                )
            observed.append(
                {"mutation": name, "test": case, "behavioral_failure_observed": True}
            )
    print(
        json.dumps(
            {
                "status": "passed",
                "evidence_class": "component_mutation",
                "clean_baseline": {"status": "passed", "required_named_tests": cases},
                "features": FEATURES,
                "mutations": observed,
                "qualification_target_unchanged": True,
            }
        )
    )


if __name__ == "__main__":
    main()
