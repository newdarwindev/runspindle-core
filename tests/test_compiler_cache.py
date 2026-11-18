"""Cache integrity/contamination tests; not real compiler or CI acceptance."""

from __future__ import annotations

import json
import os
import subprocess
import tempfile
import unittest
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch

from tools import compiler_cache as cache
from tools.mutation_target import isolated_target


class CompilerCacheTests(unittest.TestCase):
    def setUp(self) -> None:
        patcher = patch(
            "tools.prepared_builder.select",
            return_value=("linux/x86_64", {"builder_image_id": "sha256:" + "a" * 64}),
        )
        self.selected = patcher.start()
        self.addCleanup(patcher.stop)
        dependencies = patch.object(
            cache, "dependency_identity", return_value="synthetic dependencies"
        )
        self.dependency_digest = dependencies.start()
        self.addCleanup(dependencies.stop)

    def fixture(self, root: Path) -> tuple[Path, str, Path]:
        for name in (
            "Cargo.toml",
            "Cargo.lock",
            "temporal/toolchain.json",
            "tools/compiler_cache.py",
            "tools/mutation_target.py",
            "tools/prepared_builder.py",
            "src/lib.rs",
        ):
            p = root / name
            p.parent.mkdir(parents=True, exist_ok=True)
            p.write_text("synthetic")
        (root / "tests").mkdir()
        (root / "temporal/toolchain.json").write_text(
            json.dumps(
                {
                    "builders": {
                        "linux/x86_64": {
                            "builder_image_id": "sha256:" + "a" * 64,
                            "archive_config_image_id": "sha256:" + "b" * 64,
                        }
                    }
                }
            )
        )
        key = cache.key(root)
        location = root / "cache"
        seed = location / key / "target/debug/deps/dependency.rlib"
        seed.parent.mkdir(parents=True)
        location.chmod(0o700)
        (location / key).chmod(0o700)
        seed.write_bytes(b"original")
        seed.chmod(0o444)
        record = {
            "schema": 1,
            "key": key,
            "builder": "sha256:" + "a" * 64,
            "files": cache.inventory(seed.parents[2]),
        }
        (location / key / "manifest.json").write_text(json.dumps(record))
        (location / key / "manifest.json").chmod(0o444)
        workspace = root / "workspace"
        workspace.mkdir()
        return location, key, workspace

    def test_exact_source_and_builder_keys(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            _, first, _ = self.fixture(root)
            with patch.dict(os.environ, {"RUNSPINDLE_BUILDER_ARCHIVE": "/synthetic"}):
                self.assertNotEqual(first, cache.key(root))
            (root / "src/lib.rs").write_text("changed")
            self.assertNotEqual(first, cache.key(root))

    def test_two_mutants_cannot_contaminate_each_other_or_seed(self) -> None:
        with tempfile.TemporaryDirectory() as tmp, patch.object(cache, "RESERVE", 0):
            root = Path(tmp)
            location, key, workspace = self.fixture(root)
            with cache.materialized(root, location, key, workspace) as first:
                with cache.materialized(root, location, key, workspace) as second:
                    a, b = (p / "debug/deps/dependency.rlib" for p in (first, second))
                    self.assertNotEqual(a.stat().st_ino, b.stat().st_ino)
                    seed = location / key / "target/debug/deps/dependency.rlib"
                    self.assertEqual(a.stat().st_mtime_ns, seed.stat().st_mtime_ns)
                    a.write_bytes(b"mutant!!")
                    self.assertEqual(b.read_bytes(), b"original")
            self.assertFalse(first.exists())
            self.assertFalse(second.exists())
            cache.verified(location, key)

    def test_wrong_actual_builder_is_rejected(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            location, key, workspace = self.fixture(root)
            self.selected.return_value = (
                "linux/aarch64",
                {"builder_image_id": "sha256:" + "b" * 64},
            )
            with self.assertRaisesRegex(ValueError, "actual native builder"):
                with cache.materialized(root, location, key, workspace):
                    self.fail("wrong architecture admitted")

    def test_same_size_and_mtime_poison_is_detected(self) -> None:
        with tempfile.TemporaryDirectory() as tmp, patch.object(cache, "RESERVE", 0):
            root = Path(tmp)
            location, key, workspace = self.fixture(root)
            seed = location / key / "target/debug/deps/dependency.rlib"
            before = seed.stat()
            with self.assertRaisesRegex(ValueError, "changed compiler seed"):
                with cache.materialized(root, location, key, workspace):
                    seed.chmod(0o644)
                    seed.write_bytes(b"poisoned")
                    seed.chmod(0o444)
                    os.utime(seed, ns=(before.st_atime_ns, before.st_mtime_ns))

    def test_dependency_byte_identity_change_rejects_seed(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            location, key, workspace = self.fixture(root)
            self.dependency_digest.return_value = "changed dependency bytes"
            with self.assertRaisesRegex(ValueError, "does not match"):
                with cache.materialized(root, location, key, workspace):
                    self.fail("old dependency cache admitted")

    def test_unkeyed_cargo_configuration_is_rejected(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            self.fixture(root)
            (root / "build.rs").write_text("fn main() {}")
            with self.assertRaisesRegex(ValueError, "unsupported Cargo"):
                cache.key(root)
            (root / "build.rs").unlink()
            (root / ".cargo").mkdir()
            with self.assertRaisesRegex(ValueError, "unsupported Cargo"):
                cache.key(root)

    def test_directory_traversal_is_bounded_before_materialization(self) -> None:
        with tempfile.TemporaryDirectory() as tmp, patch.object(cache, "MAX_FILES", 2):
            root = Path(tmp)
            for name in ("a", "b", "c"):
                (root / name).mkdir()
            with self.assertRaisesRegex(ValueError, "traversal capacity"):
                cache.inventory(root)

    def test_timestamp_only_poison_is_rejected(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            location, key, _ = self.fixture(root)
            seed = location / key / "target/debug/deps/dependency.rlib"
            before = seed.stat()
            os.utime(seed, ns=(before.st_atime_ns, before.st_mtime_ns + 1))
            with self.assertRaisesRegex(ValueError, "changed compiler seed"):
                cache.verified(location, key)

    def test_missing_target_and_writable_manifest_rejected(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            location, key, _ = self.fixture(root)
            manifest = location / key / "manifest.json"
            manifest.chmod(0o644)
            with self.assertRaises(ValueError):
                cache.verified(location, key)
            manifest.chmod(0o444)
            (location / key / "target").rename(root / "removed-target")
            with self.assertRaisesRegex(ValueError, "target root"):
                cache.verified(location, key)

    def test_wrong_key_and_writable_seed_rejected(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            location, key, workspace = self.fixture(root)
            with self.assertRaisesRegex(ValueError, "does not match"):
                with cache.materialized(root, location, "0" * 64, workspace):
                    self.fail("wrong seed admitted")
            seed = location / key / "target/debug/deps/dependency.rlib"
            seed.chmod(0o644)
            with self.assertRaises(ValueError):
                cache.verified(location, key)

    def test_symlink_and_hardlink_rejected(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            location, key, _ = self.fixture(root)
            target = location / key / "target"
            (target / "link").symlink_to("debug")
            with self.assertRaises(ValueError):
                cache.inventory(target)
            (target / "link").unlink()
            os.link(target / "debug/deps/dependency.rlib", root / "hardlink")
            with self.assertRaises(ValueError):
                cache.inventory(target)

    def test_target_root_symlink_is_rejected(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            location, key, _ = self.fixture(root)
            target = location / key / "target"
            moved = root / "outside-target"
            target.rename(moved)
            target.symlink_to(moved, target_is_directory=True)
            with self.assertRaisesRegex(ValueError, "target root"):
                cache.verified(location, key)

    def test_uncertain_stop_preserves_owned_workspace(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            with self.assertRaises(cache.CleanupUncertain):
                with cache.preparation_workspace(Path(tmp)) as directory:
                    artifact = Path(directory) / "mounted-output"
                    artifact.write_bytes(b"preserve")
                    with patch("tools.compiler_cache.subprocess.run") as run:
                        run.side_effect = [
                            None,
                            None,
                            SimpleNamespace(stdout="still-running"),
                        ]
                        cache.run_owned(
                            ["docker", "run", "--rm", "image"], ["test"], timeout=1
                        )
            self.assertEqual(artifact.read_bytes(), b"preserve")
            self.assertTrue((Path(directory) / "cleanup-uncertain.json").is_file())

    def test_generated_internal_links_are_normalized_not_shared(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            original = root / "one"
            original.write_bytes(b"compiled")
            os.link(original, root / "two")
            with self.assertRaises(ValueError):
                cache.inventory(root)
            self.assertEqual(len(cache.inventory(root, generated=True)), 2)
            with tempfile.TemporaryDirectory() as other:
                os.link(original, Path(other) / "outside")
                with self.assertRaises(ValueError):
                    cache.inventory(root, generated=True)

    def test_actual_container_timeout_still_attempts_owned_cleanup(self) -> None:
        with patch("tools.compiler_cache.subprocess.run") as run:
            run.side_effect = [
                subprocess.TimeoutExpired("synthetic", 1),
                None,
                SimpleNamespace(stdout=""),
            ]
            with self.assertRaises(subprocess.TimeoutExpired):
                cache.run_owned(
                    ["docker", "run", "--rm", "pinned-image"], ["test"], timeout=1
                )
            launch = run.call_args_list[0].args[0]
            cleanup = run.call_args_list[1].args[0]
            self.assertIn("--kill-after=10s", launch)
            self.assertEqual(
                cleanup, ["docker", "rm", "-f", launch[launch.index("--name") + 1]]
            )

    def test_opt_in_actual_mutation_target_path_and_cleanup(self) -> None:
        with tempfile.TemporaryDirectory() as tmp, patch.object(cache, "RESERVE", 0):
            root = Path(tmp)
            location, key, workspace = self.fixture(root)
            with patch.dict(
                os.environ,
                {
                    "RUNSPINDLE_COMPILER_CACHE": str(location),
                    "RUNSPINDLE_COMPILER_CACHE_KEY": key,
                    "RUNSPINDLE_COMPILER_WORKSPACE": str(workspace),
                },
            ):
                with self.assertRaisesRegex(RuntimeError, "actual mutant failure"):
                    with isolated_target(root, "negative") as target:
                        self.assertTrue(
                            (target / "debug/deps/dependency.rlib").is_file()
                        )
                        raise RuntimeError("actual mutant failure")
                self.assertTrue(target.exists())
                self.assertTrue((target.parent / "cleanup-uncertain.json").is_file())
            cache.verified(location, key)


if __name__ == "__main__":
    unittest.main()
