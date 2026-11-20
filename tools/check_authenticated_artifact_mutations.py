#!/usr/bin/env python3
"""Require a clean baseline before behavioral artifact-evidence mutant failures.

Execute only inside the already-approved isolated runner. No fallback, pull or
qualification-target writes.
"""

from __future__ import annotations

import argparse
import json
import os
import pathlib
import re
import shutil
import subprocess
import sys
import tempfile

ROOT = pathlib.Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
from tools.mutation_target import isolated_target  # noqa: E402
from tools.prepared_builder import container_run, select  # noqa: E402

# Match the normal Rust validation feature set and existing offline lockfile.
# Literal canonical/signature vectors are independent of the Rust serializer.
FEATURES = "verified-authority,authenticated-artifacts"
mutations = [
    (
        "premature-acceptance-commit",
        "fresh_process_precommit_exit_recovers_without_partial_acceptance",
        {
            "verify(evidence, trust, clock)?; // denial rolls back all three new rows\n        tx.commit()?;": "tx.commit()?;\n        verify(evidence, trust, clock)?; // deliberately committed before final check",
        },
    ),
    (
        "untrusted-parser-diagnostic",
        "malformed_evidence_diagnostics_do_not_echo_untrusted_members_or_values",
        {
            'let evidence: Self = serde_json::from_slice(bytes)\n            .map_err(|_| anyhow::anyhow!("invalid artifact evidence encoding"))?;': "let evidence: Self = serde_json::from_slice(bytes)?;",
        },
    ),
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
    (
        "retained-artifact-links",
        "misbound_artifact_links_block_authenticated_read_and_duplicate_acceptance",
        {
            "let bytes = Store::read_accepted(connection, accepted)?;": "let bytes = Store::read_bytes(connection, accepted.artifact())?;",
        },
    ),
]
MUTATIONS_PER_SHARD = 2
SHARD_COUNT = (len(mutations) + MUTATIONS_PER_SHARD - 1) // MUTATIONS_PER_SHARD
MAX_JOIN_INPUT = 64 * 1024


def selected_mutations(shard: int | None) -> list[tuple[str, str, dict[str, str]]]:
    if shard is None:
        return mutations
    if type(shard) is not int or not 1 <= shard <= SHARD_COUNT:
        raise ValueError("invalid authenticated artifact mutation shard")
    start = (shard - 1) * MUTATIONS_PER_SHARD
    return mutations[start : start + MUTATIONS_PER_SHARD]


def baseline_cases() -> list[str]:
    return sorted({case for _, case, _ in mutations})


def checkout_sha() -> str:
    revision = subprocess.check_output(
        ["git", "-C", str(ROOT), "rev-parse", "HEAD"], text=True, timeout=8
    ).strip()
    if not re.fullmatch(r"[0-9a-f]{40}", revision):
        raise ValueError("expected exact checkout SHA")
    if subprocess.check_output(
        ["git", "-C", str(ROOT), "status", "--porcelain", "--untracked-files=all"],
        text=True,
        timeout=8,
    ):
        raise ValueError("mutation sharding requires a clean checkout")
    return revision


def coverage_record(name: str, case: str) -> dict[str, str | bool]:
    return {"mutation": name, "test": case, "behavioral_failure_observed": True}


def verify_shards(receipts: object, source_sha: str) -> dict[str, str | int]:
    """Check harness coverage after the controller verifies job provenance."""
    if not isinstance(receipts, list) or len(receipts) != SHARD_COUNT:
        raise ValueError("incomplete authenticated artifact mutation shard coverage")
    expected_proof = {
        "status": "passed",
        "source_sha": source_sha,
        "evidence_class": "component_mutation",
        "clean_baseline": {
            "status": "passed",
            "required_named_tests": baseline_cases(),
        },
        "features": FEATURES,
    }
    seen = set()
    for receipt in receipts:
        if (
            not isinstance(receipt, dict)
            or receipt.get("qualification_target_unchanged") is not True
            or any(receipt.get(key) != value for key, value in expected_proof.items())
        ):
            raise ValueError(
                "failed or mismatched authenticated artifact mutation shard"
            )
        shard = receipt.get("shard")
        if not isinstance(shard, dict) or shard.get("count") != SHARD_COUNT:
            raise ValueError("wrong authenticated artifact mutation shard count")
        index = shard.get("index")
        expected = [
            coverage_record(name, case) for name, case, _ in selected_mutations(index)
        ]
        observed = receipt.get("mutations")
        if (
            index is None
            or index in seen
            or observed != expected
            or any(
                record["behavioral_failure_observed"] is not True for record in observed
            )
        ):
            raise ValueError(
                "missing or duplicate authenticated artifact mutation case"
            )
        seen.add(index)
    return {
        "status": "passed",
        "source_sha": source_sha,
        "authenticated_artifact_mutation_shards": SHARD_COUNT,
        "authenticated_artifact_mutations": len(mutations),
    }


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


def run_mutations(shard: int | None = None) -> dict[str, object]:
    selected = selected_mutations(shard)
    revision = checkout_sha() if shard is not None else None
    source = (ROOT / "src/authenticated_artifacts.rs").read_text()
    pins = json.loads((ROOT / "temporal/toolchain.json").read_text())
    _, builder = select(pins)
    for name, _, replacements in mutations:
        for old in replacements:
            if source.count(old) != 1:
                raise AssertionError("mutation anchor drift: " + name)
    cases = baseline_cases()
    observed = []
    with isolated_target(ROOT, "artifact-evidence") as target:
        # Run the unmodified suite first in the same image, offline closure,
        # feature set, test binary and private target used by every mutant.
        baseline = run_case(target, builder, source, None)
        if baseline.returncode or any(
            f"test {case} ... ok" not in baseline.stdout for case in cases
        ):
            raise AssertionError("clean baseline failed: " + baseline.stdout[-5000:])
        for name, case, replacements in selected:
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
            observed.append(coverage_record(name, case))
    receipt: dict[str, object] = {
        "status": "passed",
        "evidence_class": "component_mutation",
        "clean_baseline": {"status": "passed", "required_named_tests": cases},
        "features": FEATURES,
        "mutations": observed,
        "qualification_target_unchanged": True,
    }
    if shard is not None:
        if checkout_sha() != revision:
            raise ValueError("checkout changed during the mutation shard")
        receipt["source_sha"] = revision
        receipt["shard"] = {"index": shard, "count": SHARD_COUNT}
    return receipt


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    mode = parser.add_mutually_exclusive_group()
    mode.add_argument("--shard", type=int, choices=range(1, SHARD_COUNT + 1))
    mode.add_argument(
        "--verify-shards",
        action="store_true",
        help="read all shard JSON receipts from stdin",
    )
    args = parser.parse_args()
    result: dict[str, object]
    from_ci = os.environ.get("RUNSPINDLE_AUTH_SHARDS") == "1"
    if args.verify_shards or from_ci:
        try:
            if from_ci:
                if args.shard is not None or os.environ.get("GITHUB_ACTIONS") != "true":
                    raise ValueError("unexpected shard join context")
                raw_cases = [
                    os.environ.get(f"RUNSPINDLE_AUTH_SHARD_{index}", "")
                    for index in range(1, SHARD_COUNT + 1)
                ]
                if sum(map(len, raw_cases)) > MAX_JOIN_INPUT:
                    raise ValueError("oversized shard receipts")
                receipts = [json.loads(raw) for raw in raw_cases]
            else:
                raw = sys.stdin.buffer.read(MAX_JOIN_INPUT + 1)
                if len(raw) > MAX_JOIN_INPUT:
                    raise ValueError("oversized shard receipts")
                receipts = json.loads(raw)
            result = dict(verify_shards(receipts, checkout_sha()))
        except (OSError, ValueError, subprocess.SubprocessError):
            parser.exit(
                2, "authenticated artifact mutation shard evidence did not validate\n"
            )
    else:
        result = run_mutations(args.shard)
    print(json.dumps(result))


if __name__ == "__main__":
    main()
