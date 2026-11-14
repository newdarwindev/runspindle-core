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
from tools.hygiene import git, source_fingerprint
from tools.runtime_artifact import validate

ROOT = Path(__file__).resolve().parents[1]


def prepared_input_digests(paths: dict[str, Path]) -> dict[str, str]:
    return {
        name: hashlib.sha256(path.read_bytes()).hexdigest()
        for name, path in paths.items()
    }


def require_prepared_inputs(paths: dict[str, Path], expected: dict[str, str]) -> None:
    if prepared_input_digests(paths) != expected:
        raise RuntimeError("prepared manifest changed during qualification")


def require_clean_sources(roots: dict[str, Path]) -> dict[str, str]:
    revisions = {}
    for name, root in roots.items():
        if git(
            root,
            "status",
            "--porcelain",
            "--untracked-files=all",
            "--ignore-submodules=none",
        ):
            raise RuntimeError("qualification source must be clean: " + name)
        revisions[name] = git(root, "rev-parse", "HEAD")
    return revisions


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
    source_roots = {
        "core": ROOT,
        "composition": Path(a.composition).resolve(),
        "platform": Path(a.platform).resolve(),
    }
    source_revisions = require_clean_sources(source_roots)
    prepared_paths = {
        "core": Path(a.prepared) / "manifest.json",
        "platform": Path(a.platform_prepared) / "prepared.json",
    }
    prepared_before = prepared_input_digests(prepared_paths)
    manifest = validate(ROOT, a.prepared)
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
            ROOT,
            [
                sys.executable,
                "tools/test_envelope_conformance.py",
                "--composition",
                str(Path(a.composition).resolve()),
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
    authority_repeat = checks[-1][2].copy()
    authority_repeat[-1] = str(reports / "authority-repeat.json")
    checks.append(("authority-repeat", ROOT, authority_repeat))
    observed: list[dict[str, Any]] = []
    summary: dict[str, Any] = {
        "status": "running",
        "evidence_class": "bounded_offline_qualification",
        "checks": observed,
        "image_id": manifest["image_id"],
        "source_hashes": manifest["source_hashes"],
        "qualification_source_fingerprints": sources_before,
        "qualification_source_revisions": source_revisions,
        "prepared_manifest_sha256": prepared_before,
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
        require_prepared_inputs(prepared_paths, prepared_before)
        if require_clean_sources(source_roots) != source_revisions:
            raise RuntimeError("qualification source revision changed during checks")
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
            ROOT / "tools/test_envelope_conformance.py",
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
