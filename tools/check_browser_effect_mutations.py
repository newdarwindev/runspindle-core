#!/usr/bin/env python3
"""Require a clean baseline before behavioral browser-effect mutant failures.

Execute only inside the already-approved isolated runner. No fallback, pull or
qualification-target writes. The untouched baseline must pass before each isolated mutation campaign.
"""

from __future__ import annotations

import json
import os
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
# Literal wire vectors are independent of the Rust serializer.
FEATURES = "verified-authority,authenticated-artifacts"
mutations = [
    (
        "response-binding",
        "malformed_unbound_future_duplicate_and_oversized_observations_stay_unknown",
        {"|| response.request_sha256 != crate::durable::digest(&request)": ""},
    ),
    (
        "post-exchange-authority",
        "post_callback_authority_loss_never_promotes_even_confirmed_response",
        {
            'anyhow::ensure!(\n            authority.verify(&self.target, &self.digest, action),\n            "browser effect authority changed"\n        );': "// deliberately omitted current-authority recheck",
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
    name = "core-browser-effect-" + uuid.uuid4().hex
    prefix = [
        "docker",
        "run",
        "--name",
        name,
        "--pull",
        "never",
        "--network",
        "none",
        "--user",
        f"{os.getuid()}:{os.getgid()}",
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
    with tempfile.TemporaryDirectory(prefix="core-browser-effect-source-") as directory:
        root = pathlib.Path(directory)
        for item in ("Cargo.toml", "Cargo.lock", "src", "tests", "fixtures"):
            original = ROOT / item
            destination = root / item
            if original.is_dir():
                shutil.copytree(original, destination)
            else:
                shutil.copyfile(original, destination)
        (root / "src/browser_effect_wire.rs").write_text(source)
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
            "browser_effect_wire",
        ]
        if case is not None:
            command += [case, "--", "--exact", "--test-threads=1"]
        else:
            command += ["--", "--test-threads=1"]
        return container_run(root, target, builder, command, 300)


def main() -> None:
    source = (ROOT / "src/browser_effect_wire.rs").read_text()
    pins = json.loads((ROOT / "temporal/toolchain.json").read_text())
    _, builder = select(pins)
    for name, _, replacements in mutations:
        for old in replacements:
            if source.count(old) != 1:
                raise AssertionError("mutation anchor drift: " + name)
    cases = sorted({case for _, case, _ in mutations})
    observed = []
    with isolated_target(ROOT, "browser-effect") as target:
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
