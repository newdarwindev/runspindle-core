"""Disposable mutants must never consume or modify another build's target."""

import os
import subprocess
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from tools.check_model_mutations import execute


class MutationCacheIsolation(unittest.TestCase):
    def test_each_copy_overrides_inherited_target_without_changing_parent(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            shared = root / "shared"
            shared.mkdir()
            sentinel = shared / "unrelated-output"
            sentinel.write_bytes(b"unchanged")
            with patch.dict(os.environ, {"CARGO_TARGET_DIR": str(shared)}):
                for name in ("first", "second"):
                    directory = root / name
                    directory.mkdir()
                    with patch(
                        "tools.check_model_mutations.subprocess.run",
                        return_value=subprocess.CompletedProcess([], 0, "", ""),
                    ) as run:
                        execute(directory, "selected_test")
                        kwargs = run.call_args.kwargs
                        self.assertEqual(
                            kwargs["env"]["CARGO_TARGET_DIR"],
                            str(directory / "target"),
                        )
                        self.assertEqual(kwargs["cwd"], directory)
                        self.assertEqual(kwargs["timeout"], 60)
                        self.assertIn("--offline", run.call_args.args[0])
                    self.assertEqual(os.environ["CARGO_TARGET_DIR"], str(shared))
            self.assertEqual(sentinel.read_bytes(), b"unchanged")

    def test_without_inherited_target_still_uses_copy_local_target(self) -> None:
        with patch.dict(os.environ, {}, clear=True):
            with patch(
                "tools.check_model_mutations.subprocess.run",
                return_value=subprocess.CompletedProcess([], 0, "", ""),
            ) as run:
                execute(Path("/synthetic-copy"))
                self.assertEqual(
                    run.call_args.kwargs["env"]["CARGO_TARGET_DIR"],
                    "/synthetic-copy/target",
                )
                self.assertNotIn("CARGO_TARGET_DIR", os.environ)
