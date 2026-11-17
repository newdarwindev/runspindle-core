"""Negative tests for the explicitly synthetic cross-language fixture boundary."""

import copy
import json
import os
from pathlib import Path
import subprocess
import sys
import tempfile
import unittest

FIXTURE = Path(__file__).parent / "fixtures" / "tether_wire_peer.py"


def canonical(value):
    return json.dumps(
        value, sort_keys=True, separators=(",", ":"), ensure_ascii=False
    ).encode()


@unittest.skipUnless(
    os.environ.get("RUNSPINDLE_TETHER_WIRE_SOURCE"),
    "explicit pinned Tether source required",
)
class ExpectedBinding(unittest.TestCase):
    def test_input_cannot_broaden_independent_expected_authority(self):
        source = os.environ.get("RUNSPINDLE_TETHER_WIRE_SOURCE")
        self.assertIsNotNone(source, "explicit pinned Tether source required")
        expected = {
            "version": "runspindle.tether-wire-bridge.v1",
            "action": "send",
            "envelope": {
                "payload": {
                    "account_id": "account-1",
                    "destination_id": "destination-1",
                }
            },
            "payload": {"text": "fixture", "attachment": {"content_hex": "6162"}},
        }
        for mutation in (
            "account",
            "destination",
            "content",
            "version",
            "action",
            "field",
        ):
            with (
                self.subTest(mutation=mutation),
                tempfile.TemporaryDirectory() as temporary,
            ):
                root = Path(temporary)
                trusted = root / "expected.json"
                trusted.write_bytes(canonical(expected))
                request = copy.deepcopy(expected)
                if mutation in ("account", "destination"):
                    request["envelope"]["payload"][mutation + "_id"] = "different"
                elif mutation == "content":
                    request["payload"]["attachment"]["content_hex"] = "6364"
                elif mutation == "field":
                    request["extra"] = True
                else:
                    request[mutation] = "unsupported"
                result = subprocess.run(
                    [
                        sys.executable,
                        str(FIXTURE),
                        "--root",
                        str(root),
                        "--expected",
                        str(trusted),
                    ],
                    input=canonical(request),
                    capture_output=True,
                    timeout=10,
                    env={
                        **os.environ,
                        "PYTHONPATH": str(source),
                        "PYTHONDONTWRITEBYTECODE": "1",
                    },
                    cwd=source,
                )
                self.assertNotEqual(result.returncode, 0)
                self.assertEqual(result.stdout, b"")
                self.assertIn(b"fixture request differs", result.stderr)
                self.assertEqual(
                    sorted(p.name for p in root.iterdir()), ["expected.json"]
                )
                self.assertEqual(trusted.read_bytes(), canonical(expected))


if __name__ == "__main__":
    unittest.main()
