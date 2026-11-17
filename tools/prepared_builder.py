"""Choose a pinned native builder; do not emulate or substitute architectures."""

import hashlib
import json
import os
import pathlib
import subprocess
import tarfile
from typing import Any, cast

ROOT = pathlib.Path(__file__).resolve().parents[1]


def select(
    pins: dict[str, Any] | None = None,
    engine: str | None = None,
    *,
    require_image: bool = True,
) -> tuple[str, dict[str, str]]:
    pins = pins or json.loads((ROOT / "temporal/toolchain.json").read_text())
    engine = (
        engine
        or subprocess.check_output(
            ["docker", "info", "--format", "{{.OSType}} {{.Architecture}}"], text=True
        ).strip()
    )
    parts = engine.split()
    if len(parts) != 2 or parts[0] != "linux":
        raise RuntimeError("unsupported Docker engine target; no emulation or fallback")
    architecture = {
        "amd64": "x86_64",
        "x86_64": "x86_64",
        "arm64": "aarch64",
        "aarch64": "aarch64",
    }.get(parts[1])
    key = f"linux/{architecture}"
    builder = pins.get("builders", {}).get(key)
    if architecture is None or builder is None:
        raise RuntimeError("no pinned native builder for Docker engine target")
    for field in (
        "builder_image_id",
        "builder_recipe_sha256",
        "protoc_sha256",
        "clippy_driver_sha256",
        "clippy_target",
    ):
        if not builder.get(field):
            raise RuntimeError(f"incomplete prepared builder pin: {field}")
    # Explicit compatibility path for the exact platform-staged Docker archive.
    # No discovery fallback, tag lookup, rebuilding, download or pin replacement.
    archive = os.environ.get("RUNSPINDLE_BUILDER_ARCHIVE")
    if archive:
        image = builder.get("archive_config_image_id")
        path = pathlib.Path(archive)
        if (
            not image
            or not path.is_absolute()
            or path.is_symlink()
            or not path.is_file()
        ):
            raise RuntimeError("unsupported builder archive selection")
        if path.stat().st_size != int(builder["archive_bytes"]):
            raise RuntimeError("builder archive size mismatch")
        with path.open("rb") as stream:
            digest = hashlib.sha256()
            for chunk in iter(lambda: stream.read(1048576), b""):
                digest.update(chunk)
            actual_digest = digest.hexdigest()
            if actual_digest != builder["closure_sha256"]:
                raise RuntimeError("builder archive digest mismatch")
            config = image.removeprefix("sha256:") + ".json"
            stream.seek(0)
            with tarfile.open(fileobj=stream, mode="r:gz") as container:
                members = container.getmembers()
                for name in ("manifest.json", config):
                    matches = [m for m in members if m.name == name]
                    if (
                        len(matches) != 1
                        or not matches[0].isfile()
                        or matches[0].size > 1048576
                    ):
                        raise RuntimeError("ambiguous or oversized archive metadata")
                manifest_stream = container.extractfile("manifest.json")
                config_stream = container.extractfile(config)
                if manifest_stream is None or config_stream is None:
                    raise RuntimeError("missing archive metadata")
                manifest = json.load(manifest_stream)
                config_bytes = config_stream.read()
                if not any(row.get("Config") == config for row in manifest):
                    raise RuntimeError("archive config is not a manifest member")
                if "sha256:" + hashlib.sha256(config_bytes).hexdigest() != image:
                    raise RuntimeError("archive image config digest mismatch")
        builder = dict(builder, builder_image_id=image)
    if not require_image:
        return key, cast(dict[str, str], builder)
    observed = subprocess.check_output(
        [
            "docker",
            "image",
            "inspect",
            builder["builder_image_id"],
            "--format",
            "{{.Id}} {{.Architecture}}",
        ],
        text=True,
    ).strip()
    image_parts = observed.split()
    image_arch = {
        "amd64": "x86_64",
        "x86_64": "x86_64",
        "arm64": "aarch64",
        "aarch64": "aarch64",
    }.get(image_parts[-1] if image_parts else "")
    if (
        len(image_parts) != 2
        or image_parts[0] != builder["builder_image_id"]
        or image_arch != architecture
    ):
        raise RuntimeError("prepared builder ID or architecture mismatch")
    return key, cast(dict[str, str], builder)
