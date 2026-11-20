"""Run Core Rust tests and Clippy using the pinned offline builder."""

from __future__ import annotations

import argparse
import os
import pathlib
import shutil
import stat
import tempfile

from tools.prepared_builder import container_run, select

ROOT = pathlib.Path(__file__).resolve().parents[1]
FEATURES = "verified-authority,authenticated-artifacts"
TARGET = "/work/local/linux-target"


GUEST_SEED = pathlib.Path("/home/runner/preparation/core-compiler-cache")
GUEST_INSTANCE = pathlib.Path("/etc/runspindle-instance.json")
GUEST_FREE_RESERVE = 8 * 1024**3
SEED_MARKER = ".runspindle-seeded-target"


def seed_guest_target(target: pathlib.Path, builder: dict[str, str]) -> bool:
    """Publish a private compiler target from the verified disposable guest seed."""
    if not GUEST_SEED.is_dir() or target.exists():
        return False
    try:
        info = GUEST_INSTANCE.lstat()
        if not stat.S_ISREG(info.st_mode) or info.st_uid != 0 or info.st_mode & 0o022:
            return False
        from tools import compiler_cache

        expected = compiler_cache.key(ROOT)
        data = compiler_cache.verified(GUEST_SEED, expected)
        if data.get("builder") != builder["builder_image_id"]:
            return False
        stage = GUEST_SEED / expected / "target"
        if (
            shutil.disk_usage(target.parent).free < GUEST_FREE_RESERVE
            or stage.stat().st_dev != target.parent.stat().st_dev
        ):
            return False
        # This extracted copy belongs to this disposable guest. The sealed
        # transport archive remains on the host.
        for relative, row in data["files"].items():
            (stage / relative).chmod(row[2] | stat.S_IWUSR)
        (stage / SEED_MARKER).touch(exist_ok=False)
        stage.rename(target)
    except (OSError, ValueError):
        print(
            "Verified compiler seed unavailable, using a clean Cargo target", flush=True
        )
        return False
    print(f"Restored verified Core compiler seed {expected}", flush=True)
    return True


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("check", choices=("test", "clippy"))
    options = parser.parse_args()
    command = [
        "cargo",
        options.check,
        "--offline",
        "--locked",
        "--features",
        FEATURES,
        "--jobs",
        "2",
        "--target-dir",
        TARGET,
    ]
    if options.check == "clippy":
        command += ["--all-targets", "--", "-D", "warnings"]
    _, builder = select()
    scratch = os.environ.get("RUNNER_TEMP")
    if scratch:
        target = pathlib.Path(scratch) / "runspindle-core-rust"
        seed_guest_target(target, builder)
        target.mkdir(mode=0o700, exist_ok=True)
        if (target / SEED_MARKER).is_file():
            command[2:2] = ["--config", "build.incremental=false"]
        result = container_run(ROOT, target, builder, command, 540)
    else:
        with tempfile.TemporaryDirectory(prefix="runspindle-core-rust-") as directory:
            result = container_run(ROOT, pathlib.Path(directory), builder, command, 540)
    print(result.stdout, end="")
    return result.returncode


if __name__ == "__main__":
    raise SystemExit(main())
