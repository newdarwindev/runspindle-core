#!/usr/bin/env python3
"""Exact-source offline Core F02 checkpoint and specified-version upgrade matrix."""

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
POINTS = {
    "authenticated": [
        "admission_commit",
        "before_child_creation",
        "after_child_creation",
        "before_reply",
    ],
    "scoped": [
        "admission_commit",
        "before_child_creation",
        "after_child_creation",
        "before_reply",
    ],
    "intent": ["admission_commit", "before_dispatch_scheduling", "before_reply"],
}


def main() -> None:
    p = argparse.ArgumentParser()
    for name in [
        "platform",
        "platform-prepared",
        "composition",
        "tether",
        "prepared",
        "prior-scoped",
        "prior-scoped-prepared",
        "prior-intent",
        "prior-intent-prepared",
        "report-dir",
    ]:
        p.add_argument("--" + name, required=True)
    a = p.parse_args()
    report = pathlib.Path(a.report_dir).resolve()
    report.mkdir(parents=True, exist_ok=True)
    roots = {
        name: pathlib.Path(getattr(a, name.replace("-", "_"))).resolve()
        for name in [
            "platform",
            "composition",
            "tether",
            "prior-scoped",
            "prior-intent",
        ]
    }
    roots["core"] = ROOT
    revisions = require_clean_sources(roots)
    fingerprints = {name: source_fingerprint(root) for name, root in roots.items()}
    pins = json.loads(
        (ROOT / "conformance/admission-checkpoints/prior.pin.json").read_text()
    )
    old = {}
    for name in ["prior-scoped", "prior-intent"]:
        prior = validate(roots[name], getattr(a, name.replace("-", "_") + "_prepared"))
        if revisions[name] != pins[name]["revision"] or any(
            prior[k] != pins[name][k]
            for k in ["image_id", "binary_sha256", "source_sha256"]
        ):
            raise RuntimeError("prior source/executable pin drift: " + name)
        old[name] = prior
    manifest = validate(ROOT, a.prepared)
    common = [
        "--platform",
        a.platform,
        "--platform-prepared",
        a.platform_prepared,
        "--prepared",
        a.prepared,
    ]
    checks: list[dict[str, Any]] = []
    summary = dict(
        status="running",
        evidence_class="bounded_isolated_admission_qualification",
        revisions=revisions,
        fingerprints=fingerprints,
        runtime=manifest,
        prior=old,
        checks=checks,
        live_acceptance="not_run",
    )

    def run(name: str, command: list[str], timeout: int = 900) -> None:
        path = report / (name + ".log")
        with path.open("w") as output:
            result = subprocess.run(
                command,
                cwd=ROOT,
                stdout=output,
                stderr=subprocess.STDOUT,
                timeout=timeout,
            )
        checks.append(
            dict(
                name=name,
                status="passed" if result.returncode == 0 else "failed",
                log_sha256=hashlib.sha256(path.read_bytes()).hexdigest(),
            )
        )
        print(name, checks[-1]["status"], flush=True)
        if result.returncode:
            raise RuntimeError("required phase failed: " + name)

    try:
        run(
            "bridge-regression",
            [
                sys.executable,
                "tools/qualify_intent_bridge.py",
                *common,
                "--composition",
                a.composition,
                "--tether",
                a.tether,
                "--report-dir",
                str(report / "bridge"),
            ],
            2400,
        )
        for repeat in range(2):
            for mode, points in POINTS.items():
                for point in points:
                    for revoked in [False, True]:
                        name = f"{mode}-{point}-revoked{int(revoked)}-run{repeat + 1}"
                        run(
                            name,
                            [
                                sys.executable,
                                "tools/test_authority_checkpoints.py",
                                *common,
                                "--mode",
                                mode,
                                "--point",
                                point,
                                "--report",
                                str(report / (name + ".json")),
                                *(["--revoke"] if revoked else []),
                            ],
                            240,
                        )
            for mode in ["authenticated", "scoped", "intent"]:
                name = f"upgrade-{mode}-run{repeat + 1}"
                if mode == "intent":
                    command = [
                        sys.executable,
                        "tools/test_intent_bridge.py",
                        *common,
                        "--tether",
                        a.tether,
                        "--prior-core",
                        a.prior_intent,
                        "--prior-prepared",
                        a.prior_intent_prepared,
                    ]
                else:
                    command = [
                        sys.executable,
                        "tools/test_verified_authority.py",
                        "--platform",
                        a.platform,
                        "--platform-prepared",
                        a.platform_prepared,
                        "--core-prepared",
                        a.prepared,
                        "--prior-core",
                        a.prior_scoped,
                        "--prior-prepared",
                        a.prior_scoped_prepared,
                        *(["--scoped"] if mode == "scoped" else []),
                    ]
                run(name, [*command, "--report", str(report / (name + ".json"))])
        for mode in ["failure", "cancel"]:
            name = "checkpoint-cleanup-" + mode
            receipt = report / (name + ".json")
            log = report / (name + ".log")
            with log.open("w") as output:
                result = subprocess.run(
                    [
                        sys.executable,
                        "tools/test_authority_checkpoints.py",
                        *common,
                        "--mode",
                        "authenticated",
                        "--point",
                        "admission_commit",
                        "--report",
                        str(report / "unused.json"),
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
            expected = (
                "deliberate checkpoint cleanup probe"
                if mode == "failure"
                else "checkpoint qualification interrupted"
            )
            if (
                not result.returncode
                or not receipt.exists()
                or expected not in log.read_text()
            ):
                raise RuntimeError("cleanup probe missed intended checkpoint")
            owned = json.loads(receipt.read_text())
            for kind, names in [
                ("container", [owned["server"], owned["worker"]]),
                ("network", [owned["network"]]),
                ("volume", [owned["volume"]]),
            ]:
                command = (
                    ["docker", "ps", "-a", "--format", "{{.Names}}"]
                    if kind == "container"
                    else ["docker", kind, "ls", "--format", "{{.Name}}"]
                )
                listed = subprocess.check_output(
                    command, text=True, timeout=30
                ).splitlines()
                if any(name in listed for name in names):
                    raise RuntimeError("probe leaked owned " + kind)
            if pathlib.Path(owned["directory"]).exists():
                raise RuntimeError("probe leaked temporary state")
            checks.append(
                dict(
                    name=name,
                    status="passed",
                    log_sha256=hashlib.sha256(log.read_bytes()).hexdigest(),
                )
            )
        if (
            require_clean_sources(roots) != revisions
            or {n: source_fingerprint(r) for n, r in roots.items()} != fingerprints
        ):
            raise RuntimeError("qualification source drift")
        validate(ROOT, a.prepared)
        summary["status"] = "passed"
    except BaseException:
        summary["status"] = "failed"
        raise
    finally:
        (report / "qualification.json").write_text(json.dumps(summary, indent=2) + "\n")


if __name__ == "__main__":
    main()
