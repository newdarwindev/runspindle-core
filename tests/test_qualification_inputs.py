"""Negative evidence for prepared input drift during authority qualification."""

from __future__ import annotations

import subprocess
import tempfile
import unittest
from pathlib import Path

from tools.qualify_verified_authority import (
    prepared_input_digests,
    require_clean_sources,
    require_prepared_inputs,
)


class PreparedInputTests(unittest.TestCase):
    def test_dirty_and_untracked_qualification_sources_are_rejected(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)

            def git(*args: str) -> str:
                return subprocess.check_output(
                    ["git", "-C", tmp, *args], text=True
                ).strip()

            git("init", "-q")
            git("config", "user.name", "Synthetic fixture")
            git("config", "user.email", "synthetic@example.invalid")
            source = root / "synthetic.txt"
            source.write_text("approved source")
            git("add", "synthetic.txt")
            git("commit", "-qm", "Synthetic source")
            self.assertEqual(
                require_clean_sources({"fixture": root}),
                {"fixture": git("rev-parse", "HEAD")},
            )
            source.write_text("changed source")
            with self.assertRaisesRegex(
                RuntimeError, "qualification source must be clean"
            ):
                require_clean_sources({"fixture": root})
            source.write_text("approved source")
            (root / "untracked.py").write_text("# unpublished helper")
            with self.assertRaisesRegex(
                RuntimeError, "qualification source must be clean"
            ):
                require_clean_sources({"fixture": root})

    def test_changed_or_missing_prepared_manifests_fail_closed(self) -> None:
        for changed in ("core", "platform"):
            with self.subTest(changed=changed), tempfile.TemporaryDirectory() as tmp:
                paths = {
                    name: Path(tmp) / (name + ".json") for name in ("core", "platform")
                }
                for path in paths.values():
                    path.write_text('{"image":"synthetic-pinned-image"}')
                before = prepared_input_digests(paths)
                require_prepared_inputs(paths, before)
                paths[changed].write_text('{"image":"changed-image"}')
                with self.assertRaisesRegex(RuntimeError, "prepared manifest changed"):
                    require_prepared_inputs(paths, before)
                paths[changed].unlink()
                with self.assertRaises(FileNotFoundError):
                    require_prepared_inputs(paths, before)
