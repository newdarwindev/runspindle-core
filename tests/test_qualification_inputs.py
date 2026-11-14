"""Negative evidence for prepared input drift during authority qualification."""

from __future__ import annotations

import tempfile
import unittest
from pathlib import Path

from tools.qualify_verified_authority import (
    prepared_input_digests,
    require_prepared_inputs,
)


class PreparedInputTests(unittest.TestCase):
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
