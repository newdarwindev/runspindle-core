#!/usr/bin/env python3
"""Deliberately break real registry code in throwaway copies; no source edits."""

import contextlib
import time
import json
import pathlib
import shutil
import subprocess
import sys
import tempfile
from mutation_target import isolated_target

ROOT = pathlib.Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
from tools.prepared_builder import select  # noqa: E402


@contextlib.contextmanager
def scratch(**kwargs):
    path = tempfile.mkdtemp(**kwargs)
    try:
        yield path
    finally:
        # Docker Desktop can briefly retain a bind mount after container exit.
        # Retry only local disposal, never a workflow or external effect.
        for attempt in range(10):
            try:
                shutil.rmtree(path)
                break
            except PermissionError:
                if attempt == 9:
                    raise
                time.sleep(0.2)


def main():
    pins = json.loads((ROOT / "temporal/toolchain.json").read_text())
    _, builder = select(pins)
    mutations = [
        (
            "capacity_disabled",
            "if self.deliveries.len() >= CAPACITY {",
            "if false {",
            "known_duplicates_reconcile_at_capacity_without_eviction_or_new_delivery",
        ),
        (
            "delivery_conflict_ignored",
            "if old == &a {",
            "if true {",
            "conflicts_revisions_and_malformed_inputs_preserve_state",
        ),
        (
            "old_operation_reused",
            ".any(|old| old.binding.operation_id == a.binding.operation_id)",
            ".any(|_old| false)",
            "any_prior_operation_id_reuse_is_rejected_after_terminal_revisions",
        ),
    ]
    observed = []
    for name, old, new, test in mutations:
        with (
            isolated_target(ROOT, name) as target,
            scratch(prefix=f"core-mutation-{name}-") as tmp,
        ):
            directory = pathlib.Path(tmp)
            for f in ["Cargo.toml", "Cargo.lock"]:
                shutil.copyfile(ROOT / f, directory / f)
            for f in ["src", "tests"]:
                shutil.copytree(ROOT / f, directory / f)
            cargo = directory / "Cargo.toml"
            text = cargo.read_text().replace(
                "[features]\n", "[features]\nmutation-no-cli = []\n"
            )
            # These registry-only tests do not exercise the CLI. Avoid linking an
            # unused executable; the actual runtime source/dependencies stay intact.
            text = text.replace(
                'required-features = ["temporal-runtime"]',
                'required-features = ["mutation-no-cli"]',
            )
            cargo.write_text(text)
            path = directory / "src/runtime.rs"
            original = path.read_text()
            if original.count(old) != 1:
                raise RuntimeError(f"mutation anchor changed: {name}")
            path.write_text(original.replace(old, new))
            command = [
                "docker",
                "run",
                "--rm",
                "--pull",
                "never",
                "--network",
                "none",
                "-v",
                f"{directory}:/source:ro",
                "-v",
                f"{target}:/work/local/linux-target",
                "-v",
                f"{pathlib.Path.home()}/.cargo/registry:/usr/local/cargo/registry:ro",
                "-v",
                f"{pathlib.Path.home()}/.cargo/git:/usr/local/cargo/git:ro",
                "-w",
                "/source",
                builder["builder_image_id"],
                "cargo",
                "test",
                "--locked",
                "--offline",
                "--features",
                "temporal-runtime",
                "--target-dir",
                "/work/local/linux-target",
                "--test",
                "runtime",
                test,
                "--",
                "--exact",
            ]
            prefix = command[: command.index("cargo")]
            subprocess.run(
                prefix
                + [
                    "cargo",
                    "clean",
                    "-p",
                    "runspindle-core",
                    "--target-dir",
                    "/work/local/linux-target",
                ],
                check=True,
                capture_output=True,
                text=True,
                timeout=60,
            )
            result = subprocess.run(
                command, capture_output=True, text=True, timeout=240
            )
            output = result.stdout + result.stderr
            detected = (
                result.returncode != 0
                and f"test {test} ... FAILED" in output
                and "could not compile" not in output
            )
            if not detected:
                raise AssertionError(
                    f"{name} was not behaviorally detected:\n{output[-4000:]}"
                )
            observed.append({"mutation": name, "test": test, "detected": True})
    print(
        json.dumps(
            {"status": "passed", "evidence_class": "component", "mutations": observed}
        )
    )


if __name__ == "__main__":
    main()
