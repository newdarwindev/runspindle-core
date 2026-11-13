#!/usr/bin/env python3
"""Offline core consumer checks for a reviewed, pinned synthetic fixture subset."""
import copy
from contextlib import contextmanager
import hashlib
import json
from pathlib import Path
import re
import socket
from types import ModuleType
from unittest.mock import patch

ROOT = Path(__file__).resolve().parents[1]
SHA256 = re.compile(r'[0-9a-f]{64}\Z')
SHA1 = re.compile(r'[0-9a-f]{40}\Z')
FILES = {'fixturekit.py', 'contract.json', 'artifact.txt', 'golden.json', 'vectors.json'}
KINDS = {'admission.observed', 'native.observed', 'artifact.observed'}


class ConsumerError(ValueError):
    pass


def require(condition, code):
    if not condition:
        raise ConsumerError(code)


def read_json(path):
    require(not path.is_symlink(), 'symlink')
    raw = path.read_bytes()
    require(len(raw) <= 65536, 'size')
    def pairs(items):
        result = {}
        for key, value in items:
            require(key not in result, 'duplicate-key')
            result[key] = value
        return result
    return json.loads(raw, object_pairs_hook=pairs), raw


@contextmanager
def deny_network():
    """Deny Python socket entrypoints; not an OS sandbox for untrusted adapters."""
    def denied(*args, **kwargs):
        raise ConsumerError('network-forbidden')
    with patch.object(socket, 'socket', denied), patch.object(socket, 'create_connection', denied), patch.object(socket, 'getaddrinfo', denied):
        yield


def load_verified_module(path, source):
    # Compile the already verified source bytes directly: an ignored/stale
    # __pycache__ must never substitute executable bytes for this reviewed source.
    module = ModuleType('pinned_core_fixturekit')
    module.__file__ = str(path)
    exec(compile(source, str(path), 'exec'), module.__dict__)
    return module


def load_pinned_kit(root=ROOT):
    """Verify reviewed pin/inventory before importing executable fixture code."""
    root = Path(root)
    fixtures = root / 'fixtures'
    bundle = fixtures / 'shared-v1'
    pin, _ = read_json(fixtures / 'shared-v1.pin.json')
    require(set(pin) == {'schema', 'component', 'fixture_schema', 'source_repository',
                        'source_commit', 'source_path', 'source_manifest_git_blob',
                        'origin_manifest_sha256', 'subset_manifest_sha256'}, 'pin-fields')
    require(pin['schema'] == 'runspindle.fixture.pin.v1'
            and pin['fixture_schema'] == 'runspindle.fixture.v1'
            and pin['component'] == 'core'
            and pin['source_repository'] == 'newdarwindev/runspindle'
            and pin['source_path'] == 'fixtures/v1', 'pin-binding')
    for name in ['source_commit', 'source_manifest_git_blob']:
        require(type(pin[name]) is str and SHA1.fullmatch(pin[name]), 'pin-revision')
    for name in ['origin_manifest_sha256', 'subset_manifest_sha256']:
        require(type(pin[name]) is str and SHA256.fullmatch(pin[name]), 'pin-digest')
    origin, raw = read_json(fixtures / 'shared-v1.origin-manifest.json')
    require(hashlib.sha256(raw).hexdigest() == pin['origin_manifest_sha256'], 'origin-pin')
    require(hashlib.sha1(b'blob ' + str(len(raw)).encode() + b'\0' + raw).hexdigest()
            == pin['source_manifest_git_blob'], 'source-blob')
    require(origin['schema'] == pin['fixture_schema']
            and set(origin['components']['core']) == KINDS, 'origin-component')
    manifest, raw = read_json(bundle / 'manifest.json')
    require(hashlib.sha256(raw).hexdigest() == pin['subset_manifest_sha256'], 'subset-pin')
    require(set(manifest) == {'schema', 'component', 'origin_manifest_sha256', 'files'}, 'subset-fields')
    require(manifest['schema'] == pin['fixture_schema'] and manifest['component'] == 'core'
            and manifest['origin_manifest_sha256'] == pin['origin_manifest_sha256'], 'subset-binding')
    require(set(manifest['files']) == FILES, 'subset-files')
    actual = set()
    for path in bundle.rglob('*'):
        require(not path.is_symlink(), 'symlink')
        if path.is_file() and '__pycache__' not in path.parts:
            actual.add(path.relative_to(bundle).as_posix())
    require(actual == FILES | {'manifest.json'}, 'subset-inventory')
    verified = {}
    for name, digest in manifest['files'].items():
        require(type(digest) is str and SHA256.fullmatch(digest), 'file-digest-format')
        verified[name] = (bundle / name).read_bytes()
        require(hashlib.sha256(verified[name]).hexdigest() == digest, 'file-digest')
    for name in ['fixturekit.py', 'contract.json', 'artifact.txt']:
        require(manifest['files'][name] == origin['files'][name], 'source-file')
    # Import only after the independently reviewed pin and file checks succeed.
    kit = load_verified_module(bundle / 'fixturekit.py', verified['fixturekit.py'])
    kit.verify_bundle(bundle)
    return kit, bundle, pin


def check_conformance(kit, bundle):
    golden = kit.load_json(bundle / 'golden.json')
    vectors = kit.load_json(bundle / 'vectors.json')
    require({x['kind'] for x in golden} == KINDS and len(golden) == 3, 'golden-kinds')
    require(len(vectors) == 9 and len({x['id'] for x in vectors}) == 9, 'vector-inventory')
    originals = {x['kind']: x for x in golden}
    digest = hashlib.sha256((bundle / 'artifact.txt').read_bytes()).hexdigest()

    def observations(original):
        # Oracle is frozen from the unmodified, pinned request fixture, never the
        # adapter output being mutated. Actual artifact bytes are independently hashed.
        if original['kind'] == 'artifact.observed':
            return {'delivered': copy.deepcopy({'identity': original['identity'], 'detail': original['detail']}),
                    'accepted_digest': digest}
        return {}

    for original in golden:
        kit.assert_conforms(original, copy.deepcopy(original['identity']), original['state'], observations(original))
    for vector in vectors:
        require(vector['kind'] in KINDS, 'vector-kind')
        original = originals[vector['kind']]
        broken = copy.deepcopy(original)
        target = broken
        path = vector['mutation']['path']
        for part in path[:-1]:
            target = target[part]
        target[path[-1]] = vector['mutation']['value']
        try:
            kit.assert_conforms(broken, copy.deepcopy(original['identity']), original['state'], observations(original))
        except kit.ContractError as error:
            require(str(error) == vector['expected_error'], 'wrong-mutation-error')
        else:
            raise ConsumerError('undetected-mutation')
    return {'golden': len(golden), 'negative_vectors_detected': len(vectors)}


def check(root=ROOT):
    with deny_network():
        kit, bundle, pin = load_pinned_kit(root)
        counts = check_conformance(kit, bundle)
    return {'status': 'passed', 'evidence_class': 'synthetic_fixture_consumer_conformance',
            'source_commit': pin['source_commit'], 'component': 'core', **counts,
            'runtime_adapter': 'not-implemented', 'integration': 'not-run', 'live_acceptance': 'not-run'}


if __name__ == '__main__':
    try:
        print(json.dumps(check(), sort_keys=True))
    except (ConsumerError, OSError, KeyError, ValueError, TypeError) as error:
        raise SystemExit('Shared fixture consumer check failed: ' + str(error))
