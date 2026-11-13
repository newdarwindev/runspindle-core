"""Core consumption of reviewed fixtures; no Rust runtime or service integration."""
import copy
import importlib.util
import json
import marshal
from pathlib import Path
import shutil
import socket
import struct
import subprocess
import sys
import tempfile
import unittest
from unittest.mock import patch

ROOT = Path(__file__).resolve().parents[1]
spec = importlib.util.spec_from_file_location('consumer', ROOT / 'tools/check_shared_fixtures.py')
consumer = importlib.util.module_from_spec(spec)
spec.loader.exec_module(consumer)


class SharedFixtures(unittest.TestCase):
    def setUp(self):
        self.guard = consumer.deny_network()
        self.guard.__enter__()
        self.addCleanup(self.guard.__exit__, None, None, None)

    def copy_consumer(self, directory):
        root = Path(directory)
        shutil.copytree(ROOT / 'fixtures', root / 'fixtures', ignore=shutil.ignore_patterns('__pycache__'))
        return root

    def assert_error(self, code, callback):
        with self.assertRaises(consumer.ConsumerError) as error:
            callback()
        self.assertEqual(str(error.exception), code)

    def edit_json(self, path, edit):
        value = json.loads(path.read_text())
        edit(value)
        path.write_text(json.dumps(value, sort_keys=True) + '\n')

    def test_pinned_core_subset_and_every_negative_vector(self):
        report = consumer.check()
        self.assertEqual(report['golden'], 3)
        self.assertEqual(report['negative_vectors_detected'], 9)
        self.assertEqual(report['runtime_adapter'], 'not-implemented')
        self.assertEqual(report['integration'], 'not-run')

    def test_consumer_needs_no_sibling_checkout_or_git_metadata(self):
        with tempfile.TemporaryDirectory() as directory:
            root = self.copy_consumer(directory)
            self.assertEqual({p.name for p in root.iterdir()}, {'fixtures'})
            self.assertEqual(consumer.check(root)['negative_vectors_detected'], 9)

    def test_fresh_process_checks_only_the_standalone_copy(self):
        with tempfile.TemporaryDirectory() as directory:
            root = self.copy_consumer(directory)
            (root / 'tools').mkdir()
            checker = root / 'tools/check_shared_fixtures.py'
            shutil.copyfile(ROOT / 'tools/check_shared_fixtures.py', checker)
            process = subprocess.run([sys.executable, '-B', str(checker)], cwd=directory,
                                     capture_output=True, text=True, timeout=10)
            self.assertEqual(process.returncode, 0, process.stderr)
            self.assertEqual(json.loads(process.stdout)['negative_vectors_detected'], 9)
            self.assertFalse((root / '.git').exists())

    def test_pin_rejects_floating_commit_and_changed_origin(self):
        for field, value, error in [
            ('source_commit', 'main', 'pin-revision'),
            ('source_repository', 'synthetic/wrong', 'pin-binding'),
            ('origin_manifest_sha256', '0'*64, 'origin-pin'),
            ('subset_manifest_sha256', '0'*64, 'subset-pin'),
            ('source_manifest_git_blob', '0'*40, 'source-blob'),
        ]:
            with self.subTest(field=field), tempfile.TemporaryDirectory() as directory:
                root = self.copy_consumer(directory)
                self.edit_json(root / 'fixtures/shared-v1.pin.json', lambda x: x.update({field: value}))
                self.assert_error(error, lambda: consumer.check(root))

    def test_manifest_or_fixture_tampering_fails_before_import(self):
        for name, error in [('manifest.json', 'subset-pin'), ('fixturekit.py', 'file-digest'), ('artifact.txt', 'file-digest')]:
            with self.subTest(name=name), tempfile.TemporaryDirectory() as directory:
                root = self.copy_consumer(directory)
                path = root / 'fixtures/shared-v1' / name
                path.write_bytes(path.read_bytes() + b' ')
                with patch.object(consumer, 'load_verified_module', side_effect=AssertionError('unverified import')):
                    self.assert_error(error, lambda: consumer.check(root))

    def test_bytecode_cache_cannot_replace_the_verified_source(self):
        with tempfile.TemporaryDirectory() as directory:
            root = self.copy_consumer(directory)
            source = root / 'fixtures/shared-v1/fixturekit.py'
            with patch.object(sys, 'pycache_prefix', None):
                cache = Path(importlib.util.cache_from_source(str(source)))
                cache.parent.mkdir()
                header = importlib.util.MAGIC_NUMBER + struct.pack('<III', 0, int(source.stat().st_mtime), source.stat().st_size)
                unreviewed = compile("raise AssertionError('unreviewed bytecode executed')", str(source), 'exec')
                cache.write_bytes(header + marshal.dumps(unreviewed))
                self.assertEqual(consumer.check(root)['negative_vectors_detected'], 9)

    def test_extra_files_are_not_part_of_the_reviewed_subset(self):
        with tempfile.TemporaryDirectory() as directory:
            root = self.copy_consumer(directory)
            (root / 'fixtures/shared-v1/unreviewed.txt').write_text('synthetic')
            self.assert_error('subset-inventory', lambda: consumer.check(root))

    def test_missing_independent_receipt_cannot_assert_delivery(self):
        kit, bundle, _ = consumer.load_pinned_kit()
        artifact = next(x for x in kit.load_json(bundle / 'golden.json') if x['kind'] == 'artifact.observed')
        with self.assertRaises(kit.ContractError) as error:
            kit.assert_conforms(artifact, artifact['identity'], 'delivered')
        self.assertEqual(str(error.exception), 'missing-independent-receipt')

    def test_vector_checker_detects_a_permissive_broken_validator(self):
        kit, bundle, _ = consumer.load_pinned_kit()
        with patch.object(kit, 'assert_conforms', return_value=True):
            self.assert_error('undetected-mutation', lambda: consumer.check_conformance(kit, bundle))

    def test_changed_output_cannot_change_the_independent_oracle(self):
        kit, bundle, _ = consumer.load_pinned_kit()
        original = kit.load_json(bundle / 'golden.json')[0]
        broken = copy.deepcopy(original)
        broken['identity']['operation_id'] = 'synthetic-other-operation'
        with self.assertRaises(kit.ContractError) as error:
            kit.assert_conforms(broken, original['identity'], original['state'])
        self.assertEqual(str(error.exception), 'identity-mismatch')

    def test_network_is_denied_before_dns_or_socket_creation(self):
        for callback in [lambda: socket.socket(), lambda: socket.create_connection(('synthetic.invalid', 443)),
                         lambda: socket.getaddrinfo('synthetic.invalid', 443)]:
            self.assert_error('network-forbidden', callback)


if __name__ == '__main__':
    unittest.main()
