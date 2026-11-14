#!/usr/bin/env python3
"""Independent Python/Rust producer/consumer conformance; no network or services."""

from __future__ import annotations

import argparse
import copy
import hashlib
import importlib.util
import json
import subprocess
import sys
import uuid
from pathlib import Path
from typing import Any

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from tools.runtime_artifact import validate

ROOT = Path(__file__).resolve().parents[1]


def test(composition: Path, prepared: str) -> dict[str, Any]:
    spec = importlib.util.spec_from_file_location(
        "wire_codec", composition / "conformance/envelope-v1/codec.py"
    )
    assert spec is not None and spec.loader is not None
    codec = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(codec)
    manifest = validate(ROOT, prepared)
    image = manifest["image_id"]
    vectors = json.loads(
        (composition / "conformance/envelope-v1/vectors.json").read_text()
    )
    assert (ROOT / "conformance/envelope-v1/vectors.json").read_bytes() == (
        composition / "conformance/envelope-v1/vectors.json"
    ).read_bytes(), "shared vector drift"

    def rust(raw: bytes) -> subprocess.CompletedProcess[bytes]:
        name = "runspindle-envelope-" + uuid.uuid4().hex
        try:
            return subprocess.run(
                [
                    "docker",
                    "run",
                    "--rm",
                    "--pull",
                    "never",
                    "--network",
                    "none",
                    "--cap-drop",
                    "ALL",
                    "--security-opt",
                    "no-new-privileges",
                    "--read-only",
                    "--pids-limit",
                    "256",
                    "--memory",
                    "1g",
                    "--cpus",
                    "2",
                    "--name",
                    name,
                    "-i",
                    image,
                    "envelope",
                ],
                input=raw,
                capture_output=True,
                timeout=40,
            )
        finally:
            subprocess.run(
                ["docker", "rm", "--force", name], capture_output=True, timeout=30
            )
            assert (
                subprocess.run(
                    ["docker", "inspect", name], capture_output=True, timeout=30
                ).returncode
                != 0
            ), "container leaked"

    for v in vectors:
        raw = codec.encode(v["envelope"])
        result = rust(raw)
        assert result.returncode == 0, result.stderr.decode()
        observed = json.loads(result.stdout)
        assert observed["component"] == v["component"], "Rust consumer mapping mismatch"
        assert codec.decode(codec.encode(observed["envelope"])) == v["envelope"], (
            "Python consumer mapping mismatch"
        )
        assert codec.component(observed["envelope"]) == observed["component"], (
            "independent Python consumer rejects Rust component mapping"
        )
        assert codec.encode(observed["envelope"]) == raw, "canonical byte mismatch"
    malformed = []
    base = vectors[0]["envelope"]
    for key, value in [("version", "future"), ("message_type", "unknown")]:
        e = copy.deepcopy(base)
        e[key] = value
        malformed.append(json.dumps(e).encode())
    for key in base:
        e = copy.deepcopy(base)
        del e[key]
        malformed.append(json.dumps(e).encode())
    for revision_value in [True, -1, 0, 1.0, 2**64]:
        e = copy.deepcopy(base)
        e["binding"]["revision"] = revision_value
        malformed.append(json.dumps(e).encode())
    e = copy.deepcopy(base)
    e["payload"]["binding"] = base["binding"]
    malformed.append(json.dumps(e).encode())
    e = copy.deepcopy(base)
    del e["payload"]["predecessor_revision"]
    malformed.append(json.dumps(e).encode())
    malformed.append(
        json.dumps(base)
        .replace('"lease_epoch": 1', '"lease_epoch": 1,"lease_epoch": 2')
        .encode()
    )
    observation = codec.encode(vectors[1]["envelope"])
    malformed += [
        observation.replace(b'"observed_at_ms":0', b'"observed_at_ms":-0'),
        b"\xef\xbb\xbf" + observation,
    ]
    for raw in malformed:
        try:
            codec.decode(raw)
        except (AssertionError, ValueError, TypeError):
            pass
        else:
            raise AssertionError("Python accepted invalid wire")
        assert rust(raw).returncode != 0, "Rust accepted invalid wire"
    return {
        "status": "passed",
        "evidence_class": "isolated_wire_conformance",
        "message_types": sorted({v["envelope"]["message_type"] for v in vectors}),
        "positive_vectors": len(vectors),
        "malformed_vectors": len(malformed),
        "independent_serializers": [
            "Python_stdlib",
            "Rust_explicit_component_mappings",
        ],
        "canonical_bytes": "equal",
        "image_id": image,
        "binary_sha256": manifest["binary_sha256"],
        "core_source_hashes": manifest["source_hashes"],
        "contract_sha256": hashlib.sha256(
            (composition / "specs/runtime-envelopes-v1.md").read_bytes()
        ).hexdigest(),
        "vectors_sha256": hashlib.sha256(
            (composition / "conformance/envelope-v1/vectors.json").read_bytes()
        ).hexdigest(),
        "cleanup": "passed",
        "live_acceptance": "not_run",
        "native_Tether_browser_console": "not_qualified",
        "independent_review": "pending_exact_candidate_review",
    }


if __name__ == "__main__":
    p = argparse.ArgumentParser()
    p.add_argument("--composition", required=True)
    p.add_argument("--prepared", required=True)
    p.add_argument("--report", required=True)
    a = p.parse_args()
    report = test(Path(a.composition).resolve(), a.prepared)
    Path(a.report).write_text(json.dumps(report, indent=2) + "\n")
    print(json.dumps(report))
