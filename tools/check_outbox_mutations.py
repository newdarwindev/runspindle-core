#!/usr/bin/env python3
"""A fabricated send acknowledgement must fail without independent readback."""

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

source = (ROOT / "src/outbox.rs").read_text()
pins = json.loads((ROOT / "temporal/toolchain.json").read_text())
_, builder = select(pins)
mutations = [
    (
        "fabricated-receipt",
        "fabricated_delivery_receipts_do_not_pass_readback",
        {"if observed.len() == 1 && observed[0] == reply {": "if true {"},
    ),
    (
        "resolution-authority",
        "conflict_resolution_requires_separate_authority_and_exact_readback",
        {"resolver.verify_resolution(resolution)": "true"},
    ),
    (
        "readback-authority",
        "revocation_during_readback_cannot_settle_delivery",
        {"authority.verify(request)": "true", "authority.verify(&old.request)": "true"},
    ),
]
expected_anchors = {
    "if observed.len() == 1 && observed[0] == reply {": 1,
    "resolver.verify_resolution(resolution)": 4,
    "authority.verify(request)": 11,
    "authority.verify(&old.request)": 1,
}
observed = []
for name, case, replacements in mutations:
    mutated = source
    for old, new in replacements.items():
        if mutated.count(old) != expected_anchors[old]:
            raise AssertionError("mutation anchor drift: " + name)
        mutated = mutated.replace(old, new)
    with (
        isolated_target(ROOT, name) as target,
        tempfile.TemporaryDirectory(prefix="core-receipt-mutation-") as directory,
    ):
        root = pathlib.Path(directory)
        for item in ("Cargo.toml", "Cargo.lock", "src", "tests"):
            p = ROOT / item
            q = root / item
            shutil.copytree(p, q) if p.is_dir() else shutil.copyfile(p, q)
        (root / "src/outbox.rs").write_text(mutated)
        prefix = [
            "docker",
            "run",
            "--rm",
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
        run = subprocess.run(
            prefix
            + [
                "cargo",
                "test",
                "--locked",
                "--offline",
                "--features",
                "delivery-runtime",
                "--target-dir",
                "/work/local/linux-target",
                "--test",
                "outbox",
                case,
                "--",
                "--exact",
            ],
            text=True,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            timeout=300,
        )
        if not (
            run.returncode
            and f"test {case} ... FAILED" in run.stdout
            and "could not compile" not in run.stdout
        ):
            raise AssertionError(run.stdout[-5000:])
        observed.append(
            {"mutation": name, "test": case, "behavioral_failure_observed": True}
        )
print(
    json.dumps(
        {
            "status": "passed",
            "evidence_class": "component_mutation",
            "mutations": observed,
            "qualification_target_unchanged": True,
        }
    )
)
