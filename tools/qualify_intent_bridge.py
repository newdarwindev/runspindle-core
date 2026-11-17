#!/usr/bin/env python3
"""Exact-source aggregate for the bounded semantic bridge; no live providers."""

from __future__ import annotations

import argparse
import hashlib
import json
import pathlib
import subprocess
import sys
from typing import Any

sys.path.insert(0, str(pathlib.Path(__file__).resolve().parents[1]))
from tools.hygiene import source_fingerprint
from tools.qualify_verified_authority import require_clean_sources
from tools.runtime_artifact import validate

ROOT = pathlib.Path(__file__).resolve().parents[1]


def main() -> None:
    p = argparse.ArgumentParser()
    for name in [
        "platform",
        "platform-prepared",
        "composition",
        "tether",
        "prepared",
        "report-dir",
    ]:
        p.add_argument("--" + name, required=True)
    a = p.parse_args()
    reports = pathlib.Path(a.report_dir).resolve()
    reports.mkdir(parents=True, exist_ok=True)
    roots = dict(
        core=ROOT,
        platform=pathlib.Path(a.platform).resolve(),
        composition=pathlib.Path(a.composition).resolve(),
        tether=pathlib.Path(a.tether).resolve(),
    )
    revisions = require_clean_sources(roots)
    fingerprints = {name: source_fingerprint(root) for name, root in roots.items()}
    pins = json.loads((ROOT / "conformance/intent-v1/tether.pin.json").read_text())
    if revisions["tether"] != pins["revision"]:
        raise RuntimeError("exact Tether candidate revision required")
    for relative, digest in pins["fixtures"].items():
        if (
            hashlib.sha256((roots["tether"] / relative).read_bytes()).hexdigest()
            != digest
        ):
            raise RuntimeError("Tether fixture pin drift")
    manifest = validate(ROOT, a.prepared)
    observed: list[dict[str, Any]] = []
    common = [
        "--platform",
        str(roots["platform"]),
        "--platform-prepared",
        a.platform_prepared,
        "--prepared",
        a.prepared,
        "--tether",
        str(roots["tether"]),
    ]
    checks = [
        (
            "legacy-and-component",
            ROOT,
            [
                sys.executable,
                "tools/qualify_verified_authority.py",
                "--platform",
                str(roots["platform"]),
                "--platform-prepared",
                a.platform_prepared,
                "--composition",
                str(roots["composition"]),
                "--prepared",
                a.prepared,
                "--report-dir",
                str(reports / "legacy"),
            ],
        ),
        (
            "tether",
            roots["tether"],
            [sys.executable, "-m", "unittest", "discover", "-s", "tests", "-v"],
        ),
        (
            "tether-optimized",
            roots["tether"],
            [sys.executable, "-O", "-m", "unittest", "discover", "-s", "tests", "-v"],
        ),
        (
            "intent-conformance",
            ROOT,
            [
                sys.executable,
                "tools/test_intent_conformance.py",
                "--tether",
                str(roots["tether"]),
                "--prepared",
                a.prepared,
                "--report",
                str(reports / "conformance.json"),
            ],
        ),
    ]
    for name in ["intent", "intent-repeat"]:
        checks.append(
            (
                name,
                ROOT,
                [
                    sys.executable,
                    "tools/test_intent_bridge.py",
                    *common,
                    "--report",
                    str(reports / (name + ".json")),
                ],
            )
        )
    summary = dict(
        status="running",
        evidence_class="bounded_offline_qualification",
        revisions=revisions,
        fingerprints=fingerprints,
        image_id=manifest["image_id"],
        checks=observed,
        live_acceptance="not_run",
        independent_review="pending_exact_candidate_review",
    )
    try:
        for name, cwd, command in checks:
            log = reports / (name + ".log")
            with log.open("w") as output:
                result = subprocess.run(
                    command,
                    cwd=cwd,
                    stdout=output,
                    stderr=subprocess.STDOUT,
                    timeout=2400,
                )
            observed.append(
                dict(
                    name=name,
                    status="passed" if result.returncode == 0 else "failed",
                    log_sha256=hashlib.sha256(log.read_bytes()).hexdigest(),
                )
            )
            if result.returncode:
                raise RuntimeError("required phase failed: " + name)
        # Deliberate failed/cancelled CLI runs must clean their own exact resources.
        for mode in ["failure", "cancel"]:
            receipt = reports / (mode + "-owned.json")
            log = reports / (mode + "-cleanup.log")
            with log.open("w") as output:
                result = subprocess.run(
                    [
                        sys.executable,
                        "tools/test_intent_bridge.py",
                        *common,
                        "--report",
                        str(reports / "unused.json"),
                        "--probe",
                        mode,
                        "--probe-report",
                        str(receipt),
                    ],
                    cwd=ROOT,
                    stdout=output,
                    stderr=subprocess.STDOUT,
                    timeout=180,
                )
            if result.returncode == 0 or not receipt.is_file():
                raise RuntimeError("cleanup probe did not reach intended failure")
            owned = json.loads(receipt.read_text())
            for kind, names in [
                ("container", [owned["server"], owned["worker"]]),
                ("volume", [owned["volume"]]),
                ("network", [owned["network"]]),
            ]:
                for name in names:
                    if (
                        subprocess.run(
                            ["docker", kind, "inspect", name],
                            capture_output=True,
                            timeout=30,
                        ).returncode
                        == 0
                    ):
                        raise RuntimeError("cleanup probe leaked " + kind)
            if pathlib.Path(owned["directory"]).exists():
                raise RuntimeError("cleanup probe leaked temporary state")
            observed.append(
                dict(
                    name=mode + "-cleanup",
                    status="passed",
                    log_sha256=hashlib.sha256(log.read_bytes()).hexdigest(),
                )
            )
        validate(ROOT, a.prepared)
        if (
            require_clean_sources(roots) != revisions
            or {name: source_fingerprint(root) for name, root in roots.items()}
            != fingerprints
        ):
            raise RuntimeError("qualification source changed")
        summary["status"] = "passed"
    except BaseException:
        summary["status"] = "failed"
        raise
    finally:
        (reports / "qualification.json").write_text(
            json.dumps(summary, indent=2) + "\n"
        )


if __name__ == "__main__":
    main()
