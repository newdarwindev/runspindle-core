"""Run Core Rust tests and Clippy using the pinned offline builder."""

from __future__ import annotations

import argparse
import os
import pathlib
import tempfile

from tools.prepared_builder import container_run, select

ROOT = pathlib.Path(__file__).resolve().parents[1]
FEATURES = "verified-authority,authenticated-artifacts"
TARGET = "/work/local/linux-target"


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
        target.mkdir(mode=0o700, exist_ok=True)
        result = container_run(ROOT, target, builder, command, 540)
    else:
        with tempfile.TemporaryDirectory(prefix="runspindle-core-rust-") as directory:
            result = container_run(ROOT, pathlib.Path(directory), builder, command, 540)
    print(result.stdout, end="")
    return result.returncode


if __name__ == "__main__":
    raise SystemExit(main())
