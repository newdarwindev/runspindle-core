"""Expected-child creation is pending, not an effect or a fabricated snapshot."""

import unittest
from unittest.mock import Mock, patch

from tools.test_verified_authority import ChildNotStarted, operation_snapshot, poll


class AuthorityReadinessTests(unittest.TestCase):
    @staticmethod
    def missing(job: str = "job-expected") -> RuntimeError:
        return RuntimeError(
            "client failed with exit 1: Error: Workflow not found\n\nCaused by:\n"
            "    code: 'Some requested entity was not found', message: "
            f'"workflow not found for ID: {job}"\n'
        )

    def test_missing_then_ready_returns_actual_snapshot(self) -> None:
        snapshot = {"state": "acknowledged", "observed": "actual"}
        query = Mock(side_effect=[self.missing(), snapshot])
        with patch("tools.test_verified_authority.time.sleep"):
            result = poll(lambda: operation_snapshot(query, "job-expected"))
        self.assertIs(result, snapshot)
        self.assertEqual(query.call_count, 2)

    def test_other_id_and_transport_errors_are_not_retried(self) -> None:
        for error in (
            self.missing("job-other"),
            RuntimeError("permission denied"),
            RuntimeError("deadline exceeded"),
            ValueError("invalid JSON"),
        ):
            query = Mock(side_effect=error)
            with self.assertRaises(type(error)) as raised:
                poll(lambda: operation_snapshot(query, "job-expected"))
            self.assertIs(raised.exception, error)
            self.assertEqual(query.call_count, 1)

    def test_expected_missing_still_expires_at_original_deadline(self) -> None:
        query = Mock(side_effect=self.missing())
        with (
            patch(
                "tools.test_verified_authority.time.monotonic", side_effect=[0, 0, 41]
            ),
            patch("tools.test_verified_authority.time.sleep"),
            self.assertRaisesRegex(
                AssertionError, "authority scenario deadline exceeded"
            ),
        ):
            poll(lambda: operation_snapshot(query, "job-expected"))
        self.assertEqual(query.call_count, 1)

    def test_direct_query_never_fabricates_an_operation(self) -> None:
        with self.assertRaises(ChildNotStarted):
            operation_snapshot(Mock(side_effect=self.missing()), "job-expected")


if __name__ == "__main__":
    unittest.main()
