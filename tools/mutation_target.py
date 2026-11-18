"""Private build targets keep mutant outputs out of qualification."""

import contextlib
import os
import pathlib
import shutil
import subprocess
import sys
import tempfile
from collections.abc import Iterator


@contextlib.contextmanager
def isolated_target(root: pathlib.Path, name: str) -> Iterator[pathlib.Path]:
    cache = os.environ.get("RUNSPINDLE_COMPILER_CACHE")
    if cache:
        from tools.compiler_cache import materialized

        expected = os.environ.get("RUNSPINDLE_COMPILER_CACHE_KEY", "")
        workspace = pathlib.Path(os.environ["RUNSPINDLE_COMPILER_WORKSPACE"])
        with materialized(root, pathlib.Path(cache), expected, workspace) as target:
            yield target
        return
    seed = (root / "local/linux-target").resolve()

    def fingerprint() -> tuple[bool, dict[str, tuple[int, int]]]:
        return seed.exists(), {
            str(p.relative_to(seed)): (p.stat().st_size, p.stat().st_mtime_ns)
            for p in seed.rglob("*")
            if p.is_file()
        }

    before = fingerprint()
    with tempfile.TemporaryDirectory(prefix="isolated-" + name + "-") as directory:
        target = pathlib.Path(directory) / "target"
        if not before[0]:
            # Fresh isolated partitions have no build outputs to reuse. Cargo
            # must compile from the pinned offline closure into this private
            # target; absence of cached outputs is not permission to skip tests.
            target.mkdir()
        elif sys.platform == "darwin":
            subprocess.run(
                ["cp", "-cR", str(seed), str(target)], check=True, timeout=120
            )
        else:
            shutil.copytree(seed, target)
        # Callers clean the component package before compiling each mutation.
        try:
            yield target
        finally:
            if fingerprint() != before:
                raise AssertionError(
                    "qualification target changed during mutation; discard results"
                )
