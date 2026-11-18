"""Negative source fences without launching Chromium or a service."""

import importlib.util
import json
import pathlib
import tempfile
import unittest
from unittest.mock import patch

FIXTURE = pathlib.Path(__file__).parent / "fixtures/browser_chromium_peer.py"


class ChromiumFixtureFence(unittest.TestCase):
    def test_changed_extra_and_symlink_files_deny_before_loading(self) -> None:
        spec = importlib.util.spec_from_file_location("chromium_fixture_fence", FIXTURE)
        if spec is None or spec.loader is None:
            self.fail("fixture missing")
        module = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(module)
        pin = json.loads(
            FIXTURE.with_name("browser_chromium_source_pin.json").read_text()
        )["files"]
        # Synthetic byte inventory tests the exact fence, not an engine claim.
        for mode in ("changed", "extra", "symlink"):
            with self.subTest(mode=mode), tempfile.TemporaryDirectory() as temporary:
                root = pathlib.Path(temporary)
                local_pin = {}
                import hashlib

                for name in pin:
                    path = root / name
                    path.parent.mkdir(parents=True, exist_ok=True)
                    path.write_bytes(b"pinned")
                    local_pin[name] = hashlib.sha256(b"pinned").hexdigest()
                target = root / "browser_runtime/__init__.py"
                if mode == "changed":
                    target.write_bytes(b"changed")
                elif mode == "extra":
                    (root / "browser_testing/extra.so").write_bytes(b"bad")
                else:
                    target.unlink()
                    target.symlink_to(root / "tools/browser_cdp.py")
                with patch.object(
                    module.json, "loads", return_value={"files": local_pin}
                ):
                    with self.assertRaises(ValueError):
                        module.load_source(root)
