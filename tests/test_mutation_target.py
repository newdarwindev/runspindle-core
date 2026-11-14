"""Synthetic filesystem checks; no compiler, Docker or runtime qualification."""

import tempfile
import unittest
from pathlib import Path

from tools.mutation_target import isolated_target


class MutationTargetTests(unittest.TestCase):
    def test_fresh_checkout_gets_private_empty_target(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            with isolated_target(root, "fresh") as target:
                self.assertTrue(target.is_dir())
                self.assertEqual(list(target.iterdir()), [])
                (target / "mutant-output").write_bytes(b"synthetic")
                self.assertFalse((root / "local").exists())
            self.assertFalse(target.exists())
            self.assertFalse((root / "local").exists())

    def test_existing_seed_is_copied_and_unchanged(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            seed = root / "local/linux-target"
            seed.mkdir(parents=True)
            original = seed / "dependency"
            original.write_bytes(b"original")
            before = original.stat()
            with isolated_target(root, "seeded") as target:
                self.assertEqual((target / "dependency").read_bytes(), b"original")
                (target / "dependency").write_bytes(b"mutated private output")
            self.assertEqual(original.read_bytes(), b"original")
            self.assertEqual(original.stat().st_mtime_ns, before.st_mtime_ns)

    def test_changed_seed_rejects_results(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            seed = root / "local/linux-target"
            seed.mkdir(parents=True)
            with self.assertRaisesRegex(AssertionError, "qualification target changed"):
                with isolated_target(root, "changed"):
                    (seed / "unrelated-write").write_bytes(b"synthetic")

    def test_new_seed_during_fresh_run_rejects_results(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            with self.assertRaisesRegex(AssertionError, "qualification target changed"):
                with isolated_target(root, "new-seed"):
                    (root / "local/linux-target").mkdir(parents=True)

    def test_failed_mutation_cleans_private_target(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            with self.assertRaisesRegex(RuntimeError, "synthetic compilation failure"):
                with isolated_target(Path(directory), "failed") as target:
                    raise RuntimeError("synthetic compilation failure")
            self.assertFalse(target.exists())


if __name__ == "__main__":
    unittest.main()
