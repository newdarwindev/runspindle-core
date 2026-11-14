"""Preserve existing acceptance triggers and clean-checkout prerequisites."""

import pathlib
import unittest

ROOT = pathlib.Path(__file__).resolve().parents[1]


class WorkflowPrerequisites(unittest.TestCase):
    def test_existing_acceptance_still_runs_on_main_push(self) -> None:
        workflow = (ROOT / ".github/workflows/orchestration-test.yml").read_text()
        self.assertRegex(workflow, r"(?m)^  push:\n    branches: \[main\]$")
        self.assertIn("  workflow_dispatch:", workflow)
        self.assertIn("tools/acceptance_gate.py orchestration-test", workflow)
