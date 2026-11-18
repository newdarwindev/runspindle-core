#!/usr/bin/env python3
"""Require a clean baseline before behavioral browser-registry mutant failures.

Execute only inside the already-approved isolated runner. No fallback, pull or
qualification-target writes. The untouched baseline must pass before each isolated mutation campaign.
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
# Process-exit and SQL observations independently verify registry invariants.
FEATURES = "verified-authority,authenticated-artifacts"
mutations = [
    (
        "reservation-after-peer",
        "fresh_process_crash_before_or_after_site_effect_never_restores_dispatch_right",
        {
            "tx.commit()?;\n        // Only this newly committed reservation may ever enter dispatch.\n        let result = client.dispatch(authority, peer)?;": "let result = client.dispatch(authority, peer)?;\n        tx.commit()?;",
        },
    ),
    (
        "replacement-operation-identity",
        "changed_operation_target_payload_and_reused_operation_deny_before_peer",
        {
            "target.binding.revision,": "target.binding.revision, &target.binding.operation,"
        },
    ),
    (
        "cached-confirmation",
        "reservation_reopen_duplicate_never_resends_or_returns_stale_confirmation",
        {"return Ok(Outcome::Unknown);": "return Ok(old.snapshot.observation);"},
    ),
    (
        "corrupt-key-inventory",
        "restored_schema_cannot_hide_corrupt_bytes_keys_or_digest",
        {"for row in rows {": "for row in rows.take(0) {"},
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
    name = "core-browser-registry-" + uuid.uuid4().hex
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
        prefix="core-browser-registry-source-"
    ) as directory:
        root = pathlib.Path(directory)
        for item in ("Cargo.toml", "Cargo.lock", "src", "tests", "fixtures"):
            original = ROOT / item
            destination = root / item
            if original.is_dir():
                shutil.copytree(original, destination)
            else:
                shutil.copyfile(original, destination)
        (root / "src/browser_effect_registry.rs").write_text(source)
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
        ]
        if case is not None:
            module = (
                "browser_registry_process"
                if case.startswith("fresh_process_")
                else "browser_effect_registry"
            )
            command += ["--test", module, case, "--", "--exact", "--test-threads=1"]
        else:
            command += [
                "--test",
                "browser_effect_registry",
                "--test",
                "browser_registry_process",
                "--",
                "--test-threads=1",
            ]
        return container_run(root, target, builder, command, 300)


def main() -> None:
    source = (ROOT / "src/browser_effect_registry.rs").read_text()
    pins = json.loads((ROOT / "temporal/toolchain.json").read_text())
    _, builder = select(pins)
    for name, _, replacements in mutations:
        for old in replacements:
            if source.count(old) != 1:
                raise AssertionError("mutation anchor drift: " + name)
    cases = sorted({case for _, case, _ in mutations})
    observed = []
    with isolated_target(ROOT, "browser-registry") as target:
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
