#!/usr/bin/env python3
"""Mutate actual artifact guards in disposable sources. Compile failures reject."""

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

source = (ROOT / "src/durable.rs").read_text()
start = source.index("        let stored: String = tx.query_row(")
end = source.index("        anyhow::ensure!(", start)
mutations = {
    "skipped_validation": (
        source[start:end],
        "        let stored = serde_json::to_string(validation)?;\n",
    ),
    "changed_bytes": ("&& digest(&bytes) == reference.sha256", "&& true"),
}
pins = json.loads((ROOT / "temporal/toolchain.json").read_text())
_, builder = select(pins)
results = []
for name, (old, new) in mutations.items():
    assert source.count(old) == 1, name
    with (
        isolated_target(ROOT, name) as target,
        tempfile.TemporaryDirectory(prefix="core-artifact-mutation-") as directory,
    ):
        root = pathlib.Path(directory)
        for name_or_path in ("Cargo.toml", "Cargo.lock", "src", "tests"):
            p = ROOT / name_or_path
            q = root / name_or_path
            shutil.copytree(p, q) if p.is_dir() else shutil.copyfile(p, q)
        (root / "src/durable.rs").write_text(source.replace(old, new))
        args = [
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
            "cargo",
            "test",
            "--locked",
            "--offline",
            "--features",
            "artifact-store",
            "--test",
            "durable_artifacts",
            "--target-dir",
            "/work/local/linux-target",
        ]
        prefix = args[: args.index("cargo")]
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
            args,
            text=True,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            timeout=300,
        )
        assert (
            run.returncode
            and "test result: FAILED" in run.stdout
            and "could not compile" not in run.stdout
        ), run.stdout[-5000:]
        results.append({"mutation": name, "behavioral_failure_observed": True})
print(
    json.dumps(
        {"evidence_class": "component_mutation", "status": "passed", "results": results}
    )
)
