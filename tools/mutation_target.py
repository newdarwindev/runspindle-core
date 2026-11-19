"""Private build targets keep mutant outputs out of qualification."""

import contextlib
import json
import os
import pathlib
import shutil
import subprocess
import sys
import tempfile
from collections.abc import Iterator


@contextlib.contextmanager
def _isolated_target(root: pathlib.Path, name: str) -> Iterator[pathlib.Path]:
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
            try:
                unchanged = fingerprint() == before
            except Exception as error:
                raise TargetIntegrityError(
                    "qualification target verification failed",
                    isinstance(error, PermissionError),
                ) from None
            if not unchanged:
                raise TargetIntegrityError(
                    "qualification target changed during mutation; discard results"
                )


class TargetIntegrityError(AssertionError):
    """The qualification-source invariant failed, independently of target cleanup."""

    def __init__(self, message: str, permission_denied: bool = False) -> None:
        super().__init__(message)
        self.permission_denied = permission_denied


@contextlib.contextmanager
def isolated_target(root: pathlib.Path, name: str) -> Iterator[pathlib.Path]:
    """Report one finite stage without changing failure/suppression semantics.

    Reports remain unverified child claims. Never print exception text or paths.
    If cleanup fails while the body is failing, the cleanup exception wins just
    as it does for the underlying context manager. Neither becomes success.
    """
    manager = _isolated_target(root, name)
    stage = "target_preparation"
    integrity_types: tuple[type[Exception], ...] = (TargetIntegrityError,)
    try:
        if os.environ.get("RUNSPINDLE_COMPILER_CACHE"):
            from tools.compiler_cache import MaterializationIntegrityError

            integrity_types += (MaterializationIntegrityError,)
        target = manager.__enter__()
        try:
            yield target
        except BaseException:
            error_info = sys.exc_info()
            stage = "target_cleanup"
            if not manager.__exit__(*error_info):
                stage = "mutation_execution"
                raise
        else:
            stage = "target_cleanup"
            manager.__exit__(None, None, None)
    except Exception as error:
        if isinstance(error, integrity_types):
            stage = "target_integrity"
        permission = isinstance(error, PermissionError) or (
            isinstance(error, integrity_types)
            and getattr(error, "permission_denied", False) is True
        )
        code = "permission_denied" if permission else "failed"
        try:
            print(
                json.dumps(
                    {
                        "type": "verification_diagnostic",
                        "version": 1,
                        "stage": stage,
                        "code": code,
                    }
                ),
                file=sys.stderr,
            )
        except Exception:
            pass  # Reporting failure must not replace the original failure.
        raise
