#!/usr/bin/env python3
"""Behavioral mutations; restore exact source bytes after every failing test."""

import hashlib
import json
from pathlib import Path
import subprocess
import signal


def cancelled(_signum, _frame):
    raise KeyboardInterrupt("mutation run cancelled")


signal.signal(signal.SIGTERM, cancelled)

ROOT = Path(__file__).resolve().parents[1]
MUTATIONS = [
    (
        "wire_sequence",
        "src/envelope.rs",
        "sequence: p.sequence,",
        "sequence: p.sequence.saturating_add(1),",
        "envelopes",
        "vectors_map_existing_component_types_and_roundtrip",
    ),
    (
        "dispatch_guard",
        "src/authenticated.rs",
        "if !self.authorized(&AuthorityCheck {",
        "if false && !self.authorized(&AuthorityCheck {",
        "authority",
        "current_revocation_stale_missing_and_conflicting_authoritative_state_deny",
    ),
]
results = []
for name, relative, old, new, suite, case in MUTATIONS:
    path = ROOT / relative
    original = path.read_bytes()
    assert original.decode().count(old) == 1, (name, "mutation target drift")
    try:
        path.write_text(original.decode().replace(old, new))
        result = subprocess.run(
            [
                "python3",
                "tools/offline_rust.py",
                "test",
                "--features",
                "verified-authority",
                "--test",
                suite,
            ],
            cwd=ROOT,
            capture_output=True,
            text=True,
            timeout=600,
        )
        output = result.stdout + result.stderr
        assert (
            result.returncode != 0
            and f"{case} ... FAILED" in output
            and "error[E" not in output
        ), (
            name,
            "mutation did not produce required behavioral failure",
            output[-3000:],
        )
        results.append(
            {
                "mutation": name,
                "status": "killed_by_behavioral_assertion",
                "test": case,
                "output_sha256": hashlib.sha256(output.encode()).hexdigest(),
            }
        )
    finally:
        path.write_bytes(original)
    assert path.read_bytes() == original
print(
    json.dumps(
        {
            "status": "passed",
            "evidence_class": "synthetic_contract",
            "mutations": results,
            "source_restoration": "passed",
            "independent_review": "user_waived_no_review_occurred",
        }
    )
)
