#!/usr/bin/env python3
"""Behavioral mutations; restore exact source bytes after every failing test."""

from __future__ import annotations

import hashlib
import json
import signal
import subprocess
from pathlib import Path
from types import FrameType


def cancelled(_signum: int, _frame: FrameType | None) -> None:
    raise KeyboardInterrupt("mutation run cancelled")


signal.signal(signal.SIGTERM, cancelled)

ROOT = Path(__file__).resolve().parents[1]
MUTATIONS = [
    (
        "ignore_authority_revocation_schema",
        "src/authority.rs",
        "        require_schema(&tx, AUTHORITY_SCHEMA)?;\n",
        "        let _ = require_schema(&tx, AUTHORITY_SCHEMA);\n",
        "authority",
        "invalid_revocation_schema_blocks_fresh_dispatch_without_effects",
    ),
    (
        "intent_dispatch_guard",
        "src/intent_runtime.rs",
        "|| !self.authorized(&AuthorityCheck {",
        "|| false && !self.authorized(&AuthorityCheck {",
        "intent",
        "signatures_scope_time_and_unavailable_trust_deny_before_effect",
    ),
    (
        "atomic_delete_pending",
        "src/intent_runtime.rs",
        "self.pending = if self.tombstone {\n            None",
        'self.pending = if self.tombstone {\n            Some(String::from_utf8(batch.intents[0].encode().expect("validated")).expect("utf8"))',
        "intent",
        "atomic_chain_never_schedules_superseded_creates_or_edits",
    ),
    (
        "single_delete_loses_pending_evidence",
        "src/intent_runtime.rs",
        "self.tombstone = true;\n            self.record_pending_superseded();",
        "self.tombstone = true;\n            self.pending = None;",
        "intent",
        "single_delete_records_only_unstarted_pending_intent_as_superseded",
    ),
    (
        "scoped_delivery_destination",
        "src/scoped_authority.rs",
        "&& request.account_id == scoped.destination.account_id",
        "&& true /* deliberately removed account binding */",
        "authority",
        "scoped_outbox_checks_exact_target_before_any_send_or_lookup",
    ),
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
            "independent_review": "pending_exact_candidate_review",
        }
    )
)
