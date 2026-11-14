"""Negative checks for native prepared builder selection."""

import json
import pathlib
import subprocess
import sys
import tempfile
import unittest
from unittest.mock import patch

from tools.prepared_builder import select

ROOT = pathlib.Path(__file__).resolve().parents[1]
PINS = json.loads((ROOT / "temporal/toolchain.json").read_text())


class BuilderSelectionTests(unittest.TestCase):
    def test_native_x64_and_arm_pins(self) -> None:
        for engine, key, docker_arch in (
            ("linux amd64", "linux/x86_64", "amd64"),
            ("linux aarch64", "linux/aarch64", "arm64"),
        ):
            image = PINS["builders"][key]["builder_image_id"]
            with (
                self.subTest(key=key),
                patch(
                    "tools.prepared_builder.subprocess.check_output",
                    return_value=f"{image} {docker_arch}\n",
                ),
            ):
                self.assertEqual(select(PINS, engine), (key, PINS["builders"][key]))

    def test_no_unpinned_or_wrong_architecture(self) -> None:
        for engine in ("linux ppc64le", "darwin amd64", "linux", "linux amd64 extra"):
            with self.subTest(engine=engine), self.assertRaises(RuntimeError):
                select(PINS, engine)
        with patch(
            "tools.prepared_builder.subprocess.check_output",
            return_value=PINS["builders"]["linux/x86_64"]["builder_image_id"]
            + " arm64\n",
        ):
            with self.assertRaisesRegex(RuntimeError, "architecture mismatch"):
                select(PINS, "linux amd64")

    def test_direct_script_loading_does_not_depend_on_working_directory(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            for script in ["prepare_temporal_runtime.py", "offline_rust.py"]:
                with self.subTest(script=script):
                    result = subprocess.run(
                        [sys.executable, str(ROOT / "tools" / script), "--help"],
                        cwd=directory,
                        capture_output=True,
                        text=True,
                        timeout=10,
                    )
                    self.assertEqual(result.returncode, 0, result.stderr)
                    self.assertIn("usage:", result.stdout)

    def test_missing_pin_fails_closed(self) -> None:
        pins = json.loads(json.dumps(PINS))
        del pins["builders"]["linux/x86_64"]["clippy_driver_sha256"]
        with self.assertRaisesRegex(RuntimeError, "incomplete"):
            select(pins, "linux amd64", require_image=False)


if __name__ == "__main__":
    unittest.main()
