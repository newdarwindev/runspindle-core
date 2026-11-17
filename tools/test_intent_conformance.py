#!/usr/bin/env python3
"""Independent Tether golden wire fixtures against pinned offline Rust consumer."""

from __future__ import annotations

import argparse
import copy
import hashlib
import json
import pathlib
import subprocess
import sys
from typing import Any

sys.path.insert(0, str(pathlib.Path(__file__).resolve().parents[1]))
from tools.runtime_artifact import validate

ROOT = pathlib.Path(__file__).resolve().parents[1]


def canonical(obj: Any) -> bytes:
    return json.dumps(
        obj, sort_keys=True, separators=(",", ":"), ensure_ascii=False
    ).encode()


def test(tether: str, prepared: str) -> dict[str, Any]:
    manifest = validate(ROOT, prepared)
    image = manifest["image_id"]
    checks = []
    pins = {}

    def run(mode: str, raw: bytes) -> subprocess.CompletedProcess[bytes]:
        return subprocess.run(
            [
                "docker",
                "run",
                "--rm",
                "--pull",
                "never",
                "--network",
                "none",
                "--read-only",
                "--cap-drop",
                "ALL",
                "--security-opt",
                "no-new-privileges",
                "--pids-limit",
                "64",
                "--memory",
                "1g",
                "--cpus",
                "1",
                "-i",
                image,
                mode,
            ],
            input=raw,
            capture_output=True,
            timeout=30,
        )

    for mode, relative, key in [
        ("intent", "conformance/intent-v1/vectors.json", "wire"),
        ("intent-batch", "conformance/intent-batch-v1/vectors.json", "batch"),
    ]:
        path = pathlib.Path(tether) / relative
        pins[relative] = hashlib.sha256(path.read_bytes()).hexdigest()
        fixture = json.loads(path.read_bytes())
        for row in fixture["vectors"]:
            raw = canonical(row[key])
            expected = row[key + "_sha256"]
            if hashlib.sha256(raw).hexdigest() != expected:
                raise AssertionError("fixture hash")
            result = run(mode, raw)
            if result.returncode or json.loads(result.stdout)["wire"].encode() != raw:
                raise AssertionError("Rust producer/consumer conformance")
            checks.append(
                dict(mode=mode, name=row["name"], sha256=expected, status="passed")
            )
        sample = fixture["vectors"][0][key]
        malformed = [
            canonical(dict(sample, version="unknown")),
            b'{"version":"a","version":"b"}',
            canonical(sample) + b" ",
        ]
        if mode == "intent":
            changed = copy.deepcopy(sample)
            changed["event"]["source"]["actor_id"] = "other"
            malformed.append(canonical(changed))
        else:
            changed = copy.deepcopy(fixture["vectors"][-1][key])
            chain = changed["intents"]
            chain[1]["event"]["event_id"] = chain[0]["event"]["event_id"]
            for index in range(1, len(chain)):
                row = chain[index]
                event = row["event"]
                request = row["scoped"]["request"]
                request["message_id"] = event["event_id"]
                request["payload"]["delivery_id"] = event["event_id"]
                event["previous"] = {
                    "request": chain[index - 1]["scoped"]["request"],
                    "wire_sha256": hashlib.sha256(
                        canonical(chain[index - 1])
                    ).hexdigest(),
                }
                request["payload"]["normalized_digest"] = hashlib.sha256(
                    canonical(event)
                ).hexdigest()
            duplicate = run(mode, canonical(changed))
            if (
                duplicate.returncode == 0
                or b"intent batch duplicate identity" not in duplicate.stderr
            ):
                raise AssertionError("resigned duplicate event bypassed identity guard")
            malformed.append(canonical(changed))
        for raw in malformed:
            if run(mode, raw).returncode == 0:
                raise AssertionError("malformed wire accepted")
        checks.append(
            dict(
                mode=mode,
                name="negative_wire_matrix",
                count=len(malformed),
                status="passed",
            )
        )
    return dict(
        status="passed",
        evidence_class="independent_cross_language_conformance",
        image_id=image,
        fixture_sha256=pins,
        checks=checks,
        network="none",
        live_acceptance="not_run",
    )


if __name__ == "__main__":
    p = argparse.ArgumentParser()
    p.add_argument("--tether", required=True)
    p.add_argument("--prepared", required=True)
    p.add_argument("--report", required=True)
    a = p.parse_args()
    result = test(a.tether, a.prepared)
    pathlib.Path(a.report).write_text(json.dumps(result, indent=2) + "\n")
    print(json.dumps(result))
