"""Embedded Python fixtures are part of exact runtime source identity."""

import hashlib
import pathlib
import sys
import tempfile
import unittest
from unittest.mock import patch

sys.path.insert(0, str(pathlib.Path(__file__).resolve().parents[1]))
from tools import prepare_temporal_runtime as build  # noqa: E402


class EmbeddedBrowserInputs(unittest.TestCase):
    def test_embedded_inputs_are_required_and_byte_bound_without_changing_old_sources(
        self,
    ) -> None:
        with tempfile.TemporaryDirectory() as temp:
            root = pathlib.Path(temp)
            for name in (
                "Cargo.toml",
                "Cargo.lock",
                "temporal/toolchain.json",
                "src/lib.rs",
            ):
                path = root / name
                path.parent.mkdir(parents=True, exist_ok=True)
                path.write_bytes(b"old")
            with patch.object(build, "ROOT", root):
                original = build.source_hashes()
                self.assertEqual(len(original), 4)
                (root / "src/browser_fixture_peer.rs").write_bytes(b"embedded helper")
                with self.assertRaises(FileNotFoundError):
                    build.source_hashes()
                names = (
                    "tests/fixtures/browser_effect_peer.py",
                    "tests/fixtures/browser_source_pin.json",
                )
                for name in names:
                    path = root / name
                    path.parent.mkdir(parents=True, exist_ok=True)
                    path.write_bytes(b"pinned")
                sealed = build.source_hashes()
                self.assertEqual(len(sealed), 7)
                for name in names:
                    self.assertEqual(
                        sealed[name], hashlib.sha256(b"pinned").hexdigest()
                    )
                    (root / name).write_bytes(b"changed")
                    self.assertNotEqual(build.source_hashes()[name], sealed[name])
                self.assertEqual({name: sealed[name] for name in original}, original)


if __name__ == "__main__":
    unittest.main()
