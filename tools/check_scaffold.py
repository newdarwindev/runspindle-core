#!/usr/bin/env python3
"""Dependency-free structural checks. Passing is not product acceptance."""
import json, pathlib, re, sys
root = pathlib.Path(__file__).resolve().parents[1]
required = ['README.md','AGENTS.md','component.json','specs/README.md','provenance.json','STATUS.json']
for name in required:
    assert (root/name).is_file(), f'Missing {name}'
c = json.loads((root/'component.json').read_text())
s = json.loads((root/'STATUS.json').read_text())
assert c['stage'] == s['stage']
assert (c['stage'], s['implementation']) in {
    ('scaffold', 'not_implemented'),
    ('contract_candidate', 'partial_contract_model'),
}
if c['stage'] == 'contract_candidate':
    for name in ['src/identity.rs', 'src/artifact.rs', 'tests/contracts.rs',
                 'specs/contracts.md', 'docs/IMPLEMENTATION_EVIDENCE.md']:
        assert (root/name).is_file(), f'Missing {name}'
    assert s['contract_tests'] in ['not_run', 'passed', 'failed']
    for name in ['native_activation', 'temporal_integration', 'external_effects']:
        assert s[name] == 'not_implemented'
assert s['live_acceptance'] == 'not_run'
p = json.loads((root/'provenance.json').read_text())
for item in p['sources']:
    assert re.fullmatch(r'SRC-[0-9]{3}', item['source_id'])
    assert item['status'] in ['adapted_spec','pending_review','reference_only']
for path in (root/'.github/workflows').glob('*.yml'):
    text = path.read_text()
    assert 'runs-on: [self-hosted, linux, x64, runspindle-isolated]' in text, path
    assert 'pull_request_target:' not in text and 'pull_request:' not in text, path
    assert 'persist-credentials: false' in text, path
    assert 'contents: read' in text, path
    assert 'ubuntu-latest' not in text and 'macos-latest' not in text and 'windows-latest' not in text, path
print(json.dumps({'status':'passed','evidence_class':'scaffold_structure','repository':c['repository'],'product_acceptance':'not_run'},sort_keys=True))
