"""Exact-source dependency seeds; never reuse component outputs or test verdicts."""

from __future__ import annotations

import argparse
import contextlib
import fcntl
import hashlib
import json
import os
import shutil
import signal
import stat
import subprocess
import tempfile
import uuid
from collections.abc import Iterator
from pathlib import Path
from typing import Any, cast

LIMIT = 32 * 1024**3
RESERVE = 30 * 1024**3
MAX_FILES = 20000
FEATURES = "verified-authority,authenticated-artifacts"


def sha(path: Path) -> str:
    result = hashlib.sha256()
    with path.open("rb") as stream:
        for chunk in iter(lambda: stream.read(1024 * 1024), b""):
            result.update(chunk)
    return result.hexdigest()


def bounded_paths(root: Path, *, limit: int | None = None) -> list[Path]:
    if root.is_symlink() or root.resolve() != root or not root.is_dir():
        raise ValueError("noncanonical compiler target root")
    bound = MAX_FILES if limit is None else limit
    pending = [root]
    observed: list[Path] = []
    while pending:
        with os.scandir(pending.pop()) as entries:
            for entry in entries:
                if len(observed) >= bound:
                    raise ValueError("compiler traversal capacity exceeded")
                path = Path(entry.path)
                info = entry.stat(follow_symlinks=False)
                if stat.S_ISLNK(info.st_mode) or not (
                    stat.S_ISREG(info.st_mode) or stat.S_ISDIR(info.st_mode)
                ):
                    raise ValueError("nonregular compiler tree entry")
                observed.append(path)
                if stat.S_ISDIR(info.st_mode):
                    pending.append(path)
    return sorted(observed)


def dependency_identity(cargo_home: Path | None = None) -> str:
    home = cargo_home or Path.home() / ".cargo"
    records = {}
    total = 0
    count = 0
    for name in ("registry", "git"):
        for path in bounded_paths(home / name, limit=50000):
            if path.is_dir():
                continue
            count += 1
            observed = path.stat()
            total += observed.st_size
            if count > 50000 or total > 4 * 1024**3:
                raise ValueError("dependency closure capacity exceeded")
            records[str(path.relative_to(home))] = [
                observed.st_size,
                sha(path),
                observed.st_mtime_ns,
            ]
    return hashlib.sha256(json.dumps(records, sort_keys=True).encode()).hexdigest()


def identity(root: Path) -> dict[str, Any]:
    unsupported = (".cargo", "build.rs", "rust-toolchain", "rust-toolchain.toml")
    if any(
        (root / name).exists() or (root / name).is_symlink() for name in unsupported
    ):
        raise ValueError("unsupported Cargo configuration or build input")
    paths = [
        root / p
        for p in (
            "Cargo.toml",
            "Cargo.lock",
            "temporal/toolchain.json",
            "tools/compiler_cache.py",
            "tools/mutation_target.py",
            "tools/prepared_builder.py",
        )
    ]
    paths += [
        p
        for d in ("src", "tests", "fixtures", "conformance")
        if (root / d).exists()
        for p in bounded_paths(root / d)
        if p.is_file() and "__pycache__" not in p.parts and p.suffix != ".pyc"
    ]
    if any(p.is_symlink() for p in paths):
        raise ValueError("source symlink refused")
    pins = json.loads((root / "temporal/toolchain.json").read_text())
    builder = pins["builders"]["linux/x86_64"]
    builder_id = (
        builder["archive_config_image_id"]
        if os.environ.get("RUNSPINDLE_BUILDER_ARCHIVE")
        else builder["builder_image_id"]
    )
    return {
        "schema": 1,
        "unsupported_inputs_absent": list(unsupported),
        "source_roots": {
            d: (root / d).exists() for d in ("src", "tests", "fixtures", "conformance")
        },
        "builder_image": builder_id,
        "dependency_bytes_sha256": dependency_identity(),
        "features": FEATURES,
        "profile": "debug",
        "incremental": False,
        "target": "linux/x86_64",
        "inputs": {str(p.relative_to(root)): sha(p) for p in sorted(paths)},
    }


def key(root: Path) -> str:
    return hashlib.sha256(
        json.dumps(identity(root), sort_keys=True).encode()
    ).hexdigest()


def inventory(root: Path, *, generated: bool = False) -> dict[str, Any]:
    if root.is_symlink() or root.resolve() != root or not root.is_dir():
        raise ValueError("noncanonical compiler target root")
    records: dict[str, Any] = {}
    total = 0
    paths = bounded_paths(root)
    links: dict[tuple[int, int], int] = {}
    if generated:
        for path in paths:
            info = path.lstat()
            if stat.S_ISREG(info.st_mode):
                inode = (info.st_dev, info.st_ino)
                links[inode] = links.get(inode, 0) + 1
    for path in paths:
        info = path.lstat()
        if stat.S_ISDIR(info.st_mode):
            continue
        if not stat.S_ISREG(info.st_mode) or info.st_nlink != (
            links.get((info.st_dev, info.st_ino), 0) if generated else 1
        ):
            raise ValueError("nonregular or shared seed inode")
        total += info.st_size
        if total > LIMIT or len(records) >= MAX_FILES:
            raise ValueError("seed capacity exceeded")
        records[str(path.relative_to(root))] = [
            info.st_size,
            sha(path),
            stat.S_IMODE(info.st_mode),
            info.st_mtime_ns,
        ]
    return records


def check_capacity(directory: Path, size: int) -> None:
    if shutil.disk_usage(directory).free - size < RESERVE:
        raise ValueError("compiler seed requires 30 GiB free reserve")


def clone(source: Path, destination: Path) -> None:
    """Distinct inode COW where supported, otherwise a full bounded copy."""
    with source.open("rb") as src, destination.open("xb") as dst:
        try:
            fcntl.ioctl(dst.fileno(), 0x40049409, src.fileno())
        except OSError:
            shutil.copyfileobj(src, dst, length=1024 * 1024)
    observed = source.stat()
    os.chmod(destination, stat.S_IMODE(observed.st_mode) | stat.S_IWUSR)
    # Cargo fingerprints compare file times; preserve them without sharing inodes.
    os.utime(destination, ns=(observed.st_atime_ns, observed.st_mtime_ns))


def verified(cache: Path, expected: str) -> dict[str, Any]:
    if (
        cache.is_symlink()
        or cache.resolve() != cache
        or not cache.is_dir()
        or cache.stat().st_uid != os.getuid()
        or stat.S_IMODE(cache.stat().st_mode) != 0o700
    ):
        raise ValueError("noncanonical compiler cache")
    entry = cache / expected
    manifest = entry / "manifest.json"
    if (
        entry.is_symlink()
        or not entry.is_dir()
        or entry.stat().st_uid != os.getuid()
        or stat.S_IMODE(entry.stat().st_mode) != 0o700
        or manifest.is_symlink()
        or not manifest.is_file()
        or manifest.stat().st_uid != os.getuid()
        or stat.S_IMODE(manifest.stat().st_mode) != 0o444
    ):
        raise ValueError("missing compiler seed")
    if manifest.stat().st_size > 8 * 1024**2:
        raise ValueError("oversized seed manifest")
    data = json.loads(manifest.read_text())
    if data.get("key") != expected or data.get("schema") != 1:
        raise ValueError("wrong compiler seed identity")
    observed = inventory(entry / "target")
    if observed != data.get("files"):
        raise ValueError("changed compiler seed bytes or mode")
    if any(row[2] & 0o222 for row in observed.values()):
        raise ValueError("writable compiler seed")
    return cast(dict[str, Any], data)


@contextlib.contextmanager
def materialized(
    root: Path, cache: Path, expected: str, workspace: Path
) -> Iterator[Path]:
    if expected != key(root):
        raise ValueError("compiler cache does not match current source and toolchain")
    data = verified(cache, expected)
    from tools.prepared_builder import select

    architecture, builder = select()
    if architecture != "linux/x86_64" or builder["builder_image_id"] != data.get(
        "builder"
    ):
        raise ValueError("compiler seed does not match actual native builder")
    size = sum(row[0] for row in data["files"].values())
    if (
        workspace.is_symlink()
        or workspace.resolve() != workspace
        or not workspace.is_dir()
    ):
        raise ValueError("noncanonical private compiler workspace")
    check_capacity(workspace, size)
    with consumer_workspace(workspace) as tmp:
        target = Path(tmp) / "target"
        target.mkdir()
        for relative in data["files"]:
            source = cache / expected / "target" / relative
            destination = target / relative
            destination.parent.mkdir(parents=True, exist_ok=True)
            clone(source, destination)
            if (
                sha(destination) != data["files"][relative][1]
                or destination.stat().st_mtime_ns != data["files"][relative][3]
            ):
                raise ValueError("compiler materialization changed bytes")
            if source.stat().st_ino == destination.stat().st_ino:
                raise ValueError("shared writable compiler inode")
        try:
            yield target
        finally:
            if verified(cache, expected) != data or key(root) != expected:
                raise ValueError("compiler seed changed during job; reject results")


@contextlib.contextmanager
def consumer_workspace(workspace: Path) -> Iterator[str]:
    directory = tempfile.mkdtemp(prefix="compiler-target-", dir=workspace)
    preserve = False
    try:
        yield directory
    except BaseException:
        # The consumer may have lost contact with its container. Do not infer
        # that an exception proves the mounted workspace is no longer in use.
        preserve = True
        (Path(directory) / "cleanup-uncertain.json").write_text(
            json.dumps(
                {
                    "status": "quarantined",
                    "reason": "consumer did not complete normally",
                }
            )
        )
        raise
    finally:
        if not preserve:
            shutil.rmtree(directory)


class CleanupUncertain(RuntimeError):
    """Owned container absence could not be proved; preserve mounted data."""


@contextlib.contextmanager
def preparation_workspace(workspace: Path) -> Iterator[str]:
    directory = tempfile.mkdtemp(prefix="prepare-compiler-", dir=workspace)
    preserve = False
    try:
        yield directory
    except CleanupUncertain as error:
        preserve = True
        (Path(directory) / "cleanup-uncertain.json").write_text(
            json.dumps({"status": "quarantined", "reason": str(error)})
        )
        raise
    finally:
        if not preserve:
            shutil.rmtree(directory)


def run_owned(prefix: list[str], command: list[str], *, timeout: int) -> None:
    """Bound the actual container workload and stop only our unique container."""
    name = "runspindle-compiler-" + uuid.uuid4().hex
    argv = (
        prefix[:3]
        + ["--name", name]
        + prefix[3:]
        + [
            "timeout",
            "--signal=TERM",
            "--kill-after=10s",
            str(timeout) + "s",
            "cargo",
            *command,
        ]
    )

    def interrupted(signum: int, frame: Any) -> None:
        raise InterruptedError("compiler preparation interrupted")

    previous = signal.signal(signal.SIGTERM, interrupted)
    try:
        subprocess.run(argv, check=True, timeout=timeout + 20)
    finally:
        try:
            subprocess.run(
                ["docker", "rm", "-f", name],
                check=False,
                stdout=subprocess.DEVNULL,
                stderr=subprocess.DEVNULL,
                timeout=30,
            )
            remaining = subprocess.run(
                [
                    "docker",
                    "container",
                    "ls",
                    "--all",
                    "--filter",
                    "name=^/" + name + "$",
                    "--format",
                    "{{.ID}}",
                ],
                check=True,
                capture_output=True,
                text=True,
                timeout=20,
            )
            if remaining.stdout.strip():
                raise CleanupUncertain(
                    "owned compiler container cleanup uncertain: " + name
                )
        except BaseException as error:
            raise CleanupUncertain(
                "owned compiler container cleanup uncertain: " + name
            ) from error
        finally:
            signal.signal(signal.SIGTERM, previous)


def prepare(root: Path, cache: Path, workspace: Path) -> dict[str, Any]:
    """Build pristine dependencies offline, remove component outputs, then seal."""
    from tools.prepared_builder import select

    expected = key(root)
    cache.mkdir(mode=0o700, parents=True, exist_ok=True)
    if (
        cache.is_symlink()
        or cache.resolve() != cache
        or cache.stat().st_uid != os.getuid()
        or stat.S_IMODE(cache.stat().st_mode) != 0o700
    ):
        raise ValueError("noncanonical compiler cache")
    if (cache / expected).exists():
        return verified(cache, expected)
    check_capacity(workspace, LIMIT)
    architecture, builder = select()
    if architecture != "linux/x86_64":
        raise ValueError("this prepared seed recipe is x86_64 only")
    with preparation_workspace(workspace) as tmp:
        target = Path(tmp) / "target"
        target.mkdir()
        source = Path(tmp) / "source"
        source.mkdir()
        # Mount only keyed build inputs, not ignored files or parent Cargo config.
        for relative, expected_hash in identity(root)["inputs"].items():
            destination = source / relative
            destination.parent.mkdir(parents=True, exist_ok=True)
            shutil.copy2(root / relative, destination)
            if sha(destination) != expected_hash:
                raise ValueError("build input changed during source materialization")
        prefix = [
            "docker",
            "run",
            "--rm",
            "--pull",
            "never",
            "--network",
            "none",
            "--user",
            f"{os.getuid()}:{os.getgid()}",
            "--cpus",
            "2",
            "--memory",
            "6g",
            "-e",
            "CARGO_INCREMENTAL=0",
            "-e",
            "CARGO_BUILD_JOBS=2",
            "-v",
            f"{source}:/source:ro",
            "-v",
            f"{target}:/work/local/linux-target",
            "-v",
            f"{Path.home()}/.cargo/registry:/usr/local/cargo/registry:ro",
            "-v",
            f"{Path.home()}/.cargo/git:/usr/local/cargo/git:ro",
            "-w",
            "/source",
            builder["builder_image_id"],
        ]
        run_owned(
            prefix,
            [
                "test",
                "--locked",
                "--offline",
                "--features",
                FEATURES,
                "--target-dir",
                "/work/local/linux-target",
                "--no-run",
            ],
            timeout=1800,
        )
        run_owned(
            prefix,
            [
                "clean",
                "-p",
                "runspindle-core",
                "--target-dir",
                "/work/local/linux-target",
            ],
            timeout=120,
        )
        # No mutable incremental state or component acceptance evidence is shared.
        shutil.rmtree(target / "debug/incremental", ignore_errors=True)
        if key(root) != expected:
            raise ValueError("source changed during compiler preparation")
        with tempfile.TemporaryDirectory(prefix=".publishing-", dir=cache) as stage:
            stage_path = Path(stage)
            records = inventory(target, generated=True)
            check_capacity(cache, sum(row[0] for row in records.values()))
            for relative in records:
                dest = stage_path / "target" / relative
                dest.parent.mkdir(parents=True, exist_ok=True)
                clone(target / relative, dest)
                os.chmod(dest, stat.S_IMODE(dest.stat().st_mode) & ~0o222)
            data = {
                "schema": 1,
                "key": expected,
                "identity": identity(root),
                "builder": builder["builder_image_id"],
                "files": inventory(stage_path / "target"),
                "tests_executed": False,
            }
            (stage_path / "manifest.json").write_text(json.dumps(data, sort_keys=True))
            os.chmod(stage_path / "manifest.json", 0o444)
            # Atomic no-overwrite directory publication; concurrent producers fail closed.
            stage_path.rename(cache / expected)
        return verified(cache, expected)


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--cache", type=Path, required=True)
    parser.add_argument("--workspace", type=Path, required=True)
    args = parser.parse_args()
    root = Path(__file__).resolve().parents[1]
    data = prepare(root, args.cache, args.workspace)
    print(
        json.dumps(
            {
                "key": data["key"],
                "files": len(data["files"]),
                "tests_executed": False,
                "ci_qualified": False,
            }
        )
    )


if __name__ == "__main__":
    main()
