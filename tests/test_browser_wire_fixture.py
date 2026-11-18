"""Explicit pinned source fence and independent synthetic grant negatives."""

import os
import shutil
import subprocess
import sys
import tempfile
import unittest
from pathlib import Path

FIXTURE = Path(__file__).parent / "fixtures/browser_effect_peer.py"


@unittest.skipUnless(
    os.environ.get("RUNSPINDLE_BROWSER_WIRE_SOURCE"), "explicit pinned Browser required"
)
class BrowserSourceFence(unittest.TestCase):
    def test_changed_or_extra_runtime_source_is_rejected_before_journal(self) -> None:
        source = Path(os.environ["RUNSPINDLE_BROWSER_WIRE_SOURCE"])
        for mutation in ("changed", "extra", "extension", "bytecode", "symlink"):
            with (
                self.subTest(mutation=mutation),
                tempfile.TemporaryDirectory() as temporary,
            ):
                root = Path(temporary)
                selected = root / "source"
                selected.mkdir()
                shutil.copytree(
                    source / "browser_runtime",
                    selected / "browser_runtime",
                    ignore=shutil.ignore_patterns("__pycache__"),
                )
                if mutation == "changed":
                    path = selected / "browser_runtime/effects.py"
                    path.write_bytes(path.read_bytes() + b"\n# changed\n")
                elif mutation == "extra":
                    (selected / "browser_runtime/untracked.py").write_text(
                        "raise RuntimeError('must not import')"
                    )
                elif mutation in ("extension", "bytecode"):
                    suffix = ".so" if mutation == "extension" else ".pyc"
                    (selected / ("browser_runtime/effects" + suffix)).write_bytes(
                        b"not admitted"
                    )
                else:
                    path = selected / "browser_runtime/effects.py"
                    path.unlink()
                    path.symlink_to(source / "browser_runtime/effects.py")
                effects = root / "effects"
                effects.mkdir()
                run = subprocess.run(
                    [
                        sys.executable,
                        str(FIXTURE),
                        "--source",
                        str(selected),
                        "--root",
                        str(effects),
                        "--action",
                        "dispatch",
                    ],
                    input=b"{}",
                    capture_output=True,
                    timeout=10,
                )
                self.assertNotEqual(run.returncode, 0)
                self.assertEqual(run.stdout, b"")
                self.assertIn(b"browser package", run.stderr)
                self.assertEqual(list(effects.iterdir()), [])


if __name__ == "__main__":
    unittest.main()
