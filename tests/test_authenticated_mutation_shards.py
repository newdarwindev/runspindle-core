import contextlib
import subprocess
import unittest
from pathlib import Path
from typing import Any
from unittest.mock import patch

from tools import check_authenticated_artifact_mutations as harness

SOURCE = "a" * 40


class AuthenticatedMutationShardsTest(unittest.TestCase):
    def recorded_shards(self) -> list[dict[str, Any]]:
        return [
            {
                "status": "passed",
                "source_sha": SOURCE,
                "evidence_class": "component_mutation",
                "clean_baseline": {
                    "status": "passed",
                    "required_named_tests": harness.baseline_cases(),
                },
                "features": harness.FEATURES,
                "qualification_target_unchanged": True,
                "shard": {"index": index, "count": harness.SHARD_COUNT},
                "mutations": [
                    harness.coverage_record(name, case)
                    for name, case, _ in harness.selected_mutations(index)
                ],
            }
            for index in range(1, harness.SHARD_COUNT + 1)
        ]

    def test_all_ten_mutations_have_one_shard(self) -> None:
        receipts = self.recorded_shards()
        self.assertEqual(
            [len(receipt["mutations"]) for receipt in receipts], [2, 2, 2, 2, 2]
        )
        self.assertEqual(
            [
                record["mutation"]
                for receipt in receipts
                for record in receipt["mutations"]
            ],
            [
                "premature-acceptance-commit",
                "untrusted-parser-diagnostic",
                "forged-validator",
                "forged-producer",
                "native-scope",
                "commit-authority",
                "byte-length",
                "retained-validation",
                "canonical-order",
                "retained-artifact-links",
            ],
        )
        self.assertEqual(
            harness.verify_shards(list(reversed(receipts)), SOURCE)[
                "authenticated_artifact_mutations"
            ],
            10,
        )
        self.assertEqual(len(harness.selected_mutations(None)), 10)

    def test_no_missing_duplicate_failed_or_stale_shard_can_pass(self) -> None:
        receipts = self.recorded_shards()
        bad_receipts = (
            receipts[:-1],
            receipts[:-1] + [receipts[0]],
            receipts[:-1] + [receipts[-1] | {"status": "cancelled"}],
            receipts[:-1] + [receipts[-1] | {"source_sha": "b" * 40}],
            receipts[:-1] + [receipts[-1] | {"mutations": []}],
            receipts[:-1] + [receipts[-1] | {"clean_baseline": {"status": "passed"}}],
        )
        for bad in bad_receipts:
            with self.subTest(bad=bad), self.assertRaises(ValueError):
                harness.verify_shards(bad, SOURCE)

    def test_shard_runs_full_baseline_before_its_cases(self) -> None:
        calls: list[tuple[Path, str | None]] = []

        def run_case(
            _target: object, _builder: object, source: Path, case: str | None
        ) -> subprocess.CompletedProcess[str]:
            calls.append((source, case))
            if case is None:
                all_pass = "\n".join(
                    f"test {name} ... ok" for name in harness.baseline_cases()
                )
                return subprocess.CompletedProcess([], 0, all_pass)
            return subprocess.CompletedProcess([], 101, f"test {case} ... FAILED")

        with (
            patch.object(harness, "checkout_sha", return_value=SOURCE),
            patch.object(harness, "select", return_value=(None, {})),
            patch.object(
                harness,
                "isolated_target",
                return_value=contextlib.nullcontext(harness.ROOT),
            ),
            patch.object(harness, "run_case", side_effect=run_case),
        ):
            receipt = harness.run_mutations(1)
        self.assertIsNone(calls[0][1])
        self.assertEqual(
            [case for _, case in calls[1:]],
            [case for _, case, _ in harness.selected_mutations(1)],
        )
        self.assertTrue(all(source != calls[0][0] for source, _ in calls[1:]))
        self.assertEqual(receipt, self.recorded_shards()[0])


if __name__ == "__main__":
    unittest.main()
