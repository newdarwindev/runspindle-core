"""Verify committed source/binary/image binding without executing the image."""

from __future__ import annotations

import hashlib
import json
import pathlib
import subprocess
import tempfile
import uuid
from typing import Any, cast


def validate(root: pathlib.Path, prepared: str | pathlib.Path) -> dict[str, Any]:
    root = pathlib.Path(root)
    declared = json.loads((root / "docs/runtime-build.json").read_text())
    manifest = json.loads((pathlib.Path(prepared) / "manifest.json").read_text())
    fields = [
        "image_id",
        "binary_sha256",
        "source_hashes",
        "dependency_inventory_sha256",
    ]
    for field in fields:
        if declared[field] != manifest[field]:
            raise RuntimeError(
                f"prepared artifact differs from committed build record: {field}"
            )
    paths = [
        root / "Cargo.toml",
        root / "Cargo.lock",
        root / "temporal/toolchain.json",
        *sorted((root / "src").rglob("*.rs")),
    ]
    source_hashes = {
        str(p.relative_to(root)): hashlib.sha256(p.read_bytes()).hexdigest()
        for p in paths
    }
    if source_hashes != declared["source_hashes"]:
        raise RuntimeError("prepared artifact source differs from this checkout")
    inventory = hashlib.sha256(
        (root / "docs/runtime-dependencies.json").read_bytes()
    ).hexdigest()
    if inventory != declared["dependency_inventory_sha256"]:
        raise RuntimeError("dependency inventory mismatch")
    image = declared["image_id"]
    metadata = json.loads(
        subprocess.check_output(
            ["docker", "image", "inspect", image], text=True, timeout=30
        )
    )[0]
    labels = metadata["Config"]["Labels"]
    expected = {
        "org.runspindle.source_sha256": hashlib.sha256(
            json.dumps(source_hashes, sort_keys=True).encode()
        ).hexdigest(),
        "org.runspindle.binary_sha256": declared["binary_sha256"],
        "org.runspindle.cargo_lock_sha256": source_hashes["Cargo.lock"],
    }
    if metadata["Id"] != image or any(labels.get(k) != v for k, v in expected.items()):
        raise RuntimeError("prepared image ID or source/binary labels mismatch")
    container = f"runspindle-core-inspect-{uuid.uuid4().hex}"
    try:
        subprocess.run(
            [
                "docker",
                "create",
                "--pull",
                "never",
                "--network",
                "none",
                "--name",
                container,
                image,
            ],
            check=True,
            capture_output=True,
            timeout=30,
        )
        with tempfile.TemporaryDirectory(prefix="core-binary-inspect-") as tmp:
            path = pathlib.Path(tmp) / "temporal-runtime"
            subprocess.run(
                [
                    "docker",
                    "cp",
                    f"{container}:/usr/local/bin/temporal-runtime",
                    str(path),
                ],
                check=True,
                capture_output=True,
                timeout=30,
            )
            if (
                hashlib.sha256(path.read_bytes()).hexdigest()
                != declared["binary_sha256"]
            ):
                raise RuntimeError("actual prepared executable bytes mismatch")
    finally:
        subprocess.run(
            ["docker", "rm", "-f", container],
            capture_output=True,
            check=True,
            timeout=30,
        )
        absent = subprocess.run(
            ["docker", "inspect", container], capture_output=True, timeout=30
        )
        if absent.returncode == 0:
            raise RuntimeError("binary inspection container leaked")
    return cast(dict[str, Any], manifest)
