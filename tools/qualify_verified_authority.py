#!/usr/bin/env python3
"""One bounded, fail-closed command for the prepared offline authority candidate."""

from __future__ import annotations

import argparse
import hashlib
import json
import subprocess
import sys
from pathlib import Path
from typing import Any

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from tools.hygiene import source_fingerprint
from tools.runtime_artifact import validate

ROOT = Path(__file__).resolve().parents[1]


def main() -> None:
    p = argparse.ArgumentParser()
    p.add_argument("--platform", required=True)
    p.add_argument("--platform-prepared", required=True)
    p.add_argument("--composition", required=True)
    p.add_argument("--prepared", default=str(ROOT / "local/runtime-prepared"))
    p.add_argument("--report-dir", required=True)
    a = p.parse_args()
    reports = Path(a.report_dir).resolve()
    reports.mkdir(parents=True, exist_ok=True)
    manifest = validate(ROOT, a.prepared)
    source_roots = {
        "core": ROOT,
        "composition": Path(a.composition).resolve(),
        "platform": Path(a.platform).resolve(),
    }
    sources_before = {
        name: source_fingerprint(root) for name, root in source_roots.items()
    }
    require = manifest.get("features") == "verified-authority"
    if not require:
        raise RuntimeError("exact verified-authority feature required")
    checks = [
        ("scaffold", ROOT, [sys.executable, "tools/check_scaffold.py"]),
        (
            "composition-scaffold",
            Path(a.composition),
            [sys.executable, "tools/check_scaffold.py"],
        ),
        (
            "python",
            ROOT,
            [sys.executable, "-m", "unittest", "discover", "-s", "tests", "-v"],
        ),
        (
            "composition-python",
            Path(a.composition),
            [sys.executable, "-m", "unittest", "discover", "-s", "tests", "-v"],
        ),
        ("rust-fmt", ROOT, ["cargo", "fmt", "--check"]),
        (
            "rust",
            ROOT,
            [
                sys.executable,
                "tools/offline_rust.py",
                "test",
                "--features",
                "verified-authority",
            ],
        ),
        (
            "strict-clippy",
            ROOT,
            [
                sys.executable,
                "tools/offline_rust.py",
                "clippy",
                "--features",
                "verified-authority",
            ],
        ),
        ("mutations", ROOT, [sys.executable, "tools/check_authority_mutations.py"]),
        (
            "wire",
            Path(a.composition),
            [
                sys.executable,
                "tools/test_envelope_conformance.py",
                "--core",
                str(ROOT),
                "--prepared",
                a.prepared,
                "--report",
                str(reports / "wire.json"),
            ],
        ),
        (
            "authority",
            ROOT,
            [
                sys.executable,
                "tools/test_verified_authority.py",
                "--platform",
                a.platform,
                "--platform-prepared",
                a.platform_prepared,
                "--core-prepared",
                a.prepared,
                "--report",
                str(reports / "authority.json"),
            ],
        ),
    ]
    observed: list[dict[str, Any]] = []
    summary: dict[str, Any] = {
        "status": "running",
        "evidence_class": "bounded_offline_qualification",
        "checks": observed,
        "image_id": manifest["image_id"],
        "source_hashes": manifest["source_hashes"],
        "qualification_source_fingerprints": sources_before,
        "independent_review": "pending_exact_candidate_review",
        "live_acceptance": "not_run",
    }
    try:
        for name, cwd, command in checks:
            with (reports / (name + ".log")).open("w") as output:
                result = subprocess.run(
                    command,
                    cwd=cwd,
                    stdout=output,
                    stderr=subprocess.STDOUT,
                    timeout=1800,
                )
            observed.append(
                {
                    "name": name,
                    "status": "passed" if result.returncode == 0 else "failed",
                    "log_sha256": hashlib.sha256(
                        (reports / (name + ".log")).read_bytes()
                    ).hexdigest(),
                }
            )
            if result.returncode:
                raise RuntimeError("required qualification failed: " + name)
        validate(ROOT, a.prepared)
        if sources_before != {
            name: source_fingerprint(root) for name, root in source_roots.items()
        }:
            raise RuntimeError("qualification source changed during checks")
        summary["status"] = "passed"
    except BaseException:
        summary["status"] = "failed"
        raise
    finally:
        paths = [
            *ROOT.glob("tools/*authority*.py"),
            ROOT / "tests/authority.rs",
            ROOT / "tests/envelopes.rs",
            Path(a.composition) / "conformance/envelope-v1/codec.py",
            Path(a.composition) / "conformance/envelope-v1/vectors.json",
            Path(a.platform_prepared) / "prepared.json",
        ]
        summary["test_and_config_hashes"] = {
            str(path): hashlib.sha256(path.read_bytes()).hexdigest()
            for path in paths
            if path.is_file()
        }
        (reports / "qualification.json").write_text(
            json.dumps(summary, indent=2) + "\n"
        )
    print(json.dumps(summary))


if __name__ == "__main__":
    main()
