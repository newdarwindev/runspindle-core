"""Diagnostic-only synthetic failures; no Docker, VM, compiler or permissions."""

import contextlib
import io
import json
import tempfile
import unittest
from pathlib import Path
from typing import Any
from unittest.mock import MagicMock, patch

from tools import hygiene, mutation_target


class MutationDiagnosticTests(unittest.TestCase):
    def exercise(
        self, manager: MagicMock, body_error: Exception | None = None
    ) -> tuple[dict[str, Any], Exception]:
        output = io.StringIO()
        with patch.object(mutation_target, "_isolated_target", return_value=manager):
            with contextlib.redirect_stderr(output):
                with self.assertRaises(Exception) as caught:
                    with mutation_target.isolated_target(
                        Path("PRIVATE_PATH"), "PRIVATE_NAME"
                    ):
                        if body_error is not None:
                            raise body_error
        lines = output.getvalue().splitlines()
        self.assertEqual(len(lines), 1)
        self.assertNotIn("PRIVATE", output.getvalue())
        value = json.loads(lines[0])
        self.assertEqual(set(value), {"type", "version", "stage", "code"})
        parsed = hygiene.reported_failure(output.getvalue())
        self.assertEqual(parsed["provenance"], "child_output_unverified")
        self.assertEqual(parsed["category"], "checker_report")
        self.assertEqual(parsed["stage"], value["stage"])
        return value, caught.exception

    def test_preparation_permission_denial(self) -> None:
        manager = MagicMock()
        error = PermissionError("PRIVATE_PATH")
        manager.__enter__.side_effect = error
        value, thrown = self.exercise(manager)
        self.assertEqual(value["stage"], "target_preparation")
        self.assertEqual(value["code"], "permission_denied")
        self.assertIs(thrown, error)
        manager.__exit__.assert_not_called()

    def test_execution_failure_is_not_cleanup_failure(self) -> None:
        manager = MagicMock()
        manager.__exit__.return_value = False
        error = RuntimeError("PRIVATE_OUTPUT")
        value, thrown = self.exercise(manager, error)
        self.assertEqual(value["stage"], "mutation_execution")
        self.assertIs(thrown, error)
        manager.__exit__.assert_called_once()

    def test_cleanup_permission_denial_overrides_body_once(self) -> None:
        for body in (None, RuntimeError("PRIVATE_BODY")):
            with self.subTest(body=body is None):
                manager = MagicMock()
                error = PermissionError("PRIVATE_CLEANUP")
                manager.__exit__.side_effect = error
                value, thrown = self.exercise(manager, body)
                self.assertEqual(value["stage"], "target_cleanup")
                self.assertEqual(value["code"], "permission_denied")
                self.assertIs(thrown, error)
                manager.__exit__.assert_called_once()

    def test_integrity_failure_not_cleanup(self) -> None:
        manager = MagicMock()
        manager.__exit__.side_effect = mutation_target.TargetIntegrityError("PRIVATE")
        value, _ = self.exercise(manager)
        self.assertEqual(value["stage"], "target_integrity")

    def test_real_seed_integrity_check_stays_enforced(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            output = io.StringIO()
            with contextlib.redirect_stderr(output):
                with self.assertRaises(AssertionError):
                    with mutation_target.isolated_target(root, "changed"):
                        (root / "local/linux-target").mkdir(parents=True)
            self.assertEqual(json.loads(output.getvalue())["stage"], "target_integrity")
            self.assertNotIn(directory, output.getvalue())

    def test_success_emits_nothing_and_cleans(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            output = io.StringIO()
            with contextlib.redirect_stderr(output):
                with mutation_target.isolated_target(
                    Path(directory), "success"
                ) as target:
                    self.assertTrue(target.is_dir())
            self.assertFalse(target.exists())
            self.assertEqual(output.getvalue(), "")

    def test_reporting_failure_keeps_original_error(self) -> None:
        manager = MagicMock()
        error = PermissionError("PRIVATE")
        manager.__enter__.side_effect = error
        with (
            patch.object(mutation_target, "_isolated_target", return_value=manager),
            patch("builtins.print", side_effect=OSError("PRIVATE")),
        ):
            with self.assertRaises(PermissionError) as caught:
                with mutation_target.isolated_target(Path("unused"), "x"):
                    self.fail("entered")
        self.assertIs(caught.exception, error)

    def test_cleanup_wins_integrity_error(self) -> None:
        manager = MagicMock()
        manager.__exit__.side_effect = PermissionError("PRIVATE")
        value, _ = self.exercise(
            manager, mutation_target.TargetIntegrityError("PRIVATE")
        )
        self.assertEqual(value["stage"], "target_cleanup")

    def test_suppression_and_cancellation_contract(self) -> None:
        for suppressed in (True, False):
            manager = MagicMock()
            manager.__exit__.return_value = suppressed
            output = io.StringIO()
            interruption = KeyboardInterrupt()
            with (
                patch.object(mutation_target, "_isolated_target", return_value=manager),
                contextlib.redirect_stderr(output),
            ):

                def invoke() -> None:
                    with mutation_target.isolated_target(Path("unused"), "x"):
                        raise interruption

                if suppressed:
                    invoke()
                else:
                    with self.assertRaises(KeyboardInterrupt) as caught:
                        invoke()
                    self.assertIs(caught.exception, interruption)
            manager.__exit__.assert_called_once()
            self.assertEqual(output.getvalue(), "")

    def test_cache_body_propagation_and_integrity_marker(self) -> None:
        from tools.compiler_cache import MaterializationIntegrityError

        for failure, stage in (
            (RuntimeError("PRIVATE"), "mutation_execution"),
            (MaterializationIntegrityError("PRIVATE", True), "target_integrity"),
        ):
            manager = MagicMock()
            manager.__exit__.return_value = False
            error = None
            if stage == "target_integrity":
                manager.__exit__.side_effect = failure
            else:
                error = failure
            with patch.dict(
                "os.environ",
                {
                    "RUNSPINDLE_COMPILER_CACHE": "PRIVATE",
                    "RUNSPINDLE_COMPILER_CACHE_KEY": "PRIVATE",
                    "RUNSPINDLE_COMPILER_WORKSPACE": "PRIVATE",
                },
            ):
                value, thrown = self.exercise(manager, error)
            self.assertIs(thrown, failure)
            self.assertEqual(value["stage"], stage)
            if stage == "target_integrity":
                self.assertEqual(value["code"], "permission_denied")

    def test_multiple_claims_remain_unclassified(self) -> None:
        claim = json.dumps(
            {
                "type": "verification_diagnostic",
                "version": 1,
                "stage": "target_cleanup",
                "code": "permission_denied",
            }
        )
        self.assertEqual(
            hygiene.reported_failure(claim + "\n" + claim)["category"], "unclassified"
        )


if __name__ == "__main__":
    unittest.main()
