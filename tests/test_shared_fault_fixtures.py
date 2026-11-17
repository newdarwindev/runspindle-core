"""Reviewed fixture-only schedule consumption; not runtime or durable recovery."""

from __future__ import annotations

import copy
import importlib.util
import socket
import unittest
from pathlib import Path
from typing import Any
from unittest.mock import patch

ROOT = Path(__file__).resolve().parents[1]
BUNDLE = ROOT / "fixtures/shared-v1"
SPEC = importlib.util.spec_from_file_location(
    "consumer_fault_check", ROOT / "tools/check_shared_fixtures.py"
)
assert SPEC is not None and SPEC.loader is not None
consumer = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(consumer)
kit, _, _ = consumer.load_pinned_kit()


class FaultConsumerTests(unittest.TestCase):
    def setUp(self) -> None:
        for method in ("socket", "create_connection", "getaddrinfo"):
            guard = patch.object(
                socket, method, side_effect=AssertionError("network forbidden")
            )
            spy = guard.start()
            self.addCleanup(guard.stop)
            self.addCleanup(spy.assert_not_called)

    def test_malformed_expected_identity_and_receipt_rejected(self) -> None:
        for fixture in kit.load_json(BUNDLE / "golden.json"):
            expected = copy.deepcopy(fixture["identity"])
            expected["attempt"] = True
            with self.assertRaises(kit.ContractError):
                kit.assert_conforms(fixture, expected, fixture["state"])
            if fixture["state"] in ("delivered", "persisted", "serving"):
                receipt = {
                    "identity": copy.deepcopy(fixture["identity"]),
                    "detail": copy.deepcopy(fixture["detail"]),
                }
                receipt["identity"]["attempt"] = True
                observations = {
                    fixture["state"]: receipt,
                    "accepted_digest": fixture["identity"].get("artifact_digest"),
                    "approved_digest": fixture["identity"].get("candidate_digest"),
                }
                with self.assertRaises(kit.ContractError):
                    kit.assert_conforms(
                        fixture, fixture["identity"], fixture["state"], observations
                    )

    def test_fault_traces_from_separate_model_effect_spy(self) -> None:
        # Executes a deliberately small in-memory model. No durable recovery claim.
        cases = kit.load_json(BUNDLE / "faults.json")["cases"]
        labels = {
            "duplicate": "same-task-and-operation",
            "reorder": "older-observation-cannot-replace-newer",
            "timeout": "unknown-no-blind-retry",
            "partial-response": "unknown-no-delivered-state",
            "retry": "new-attempt-same-operation",
            "restart": "blocked-until-real-durable-adapter",
            "uncertain-effect": "unknown-no-resubmit",
        }
        for case in cases:
            for defect in (
                None,
                "extra-effect",
                "wrong-order",
                "regress-sequence",
                "unauthorized-retry",
                "wrong-effect-identity",
            ):
                spy = kit.Spy(["send", "commit"])
                identity = {
                    "task_id": "synthetic-task",
                    "operation_id": "synthetic-operation",
                }
                calls: list[dict[str, Any]] = []
                steps: list[dict[str, Any]] = []
                state, attempt, sequence = "new", 1, 0
                seen: set[str] = set()

                def effect(operation: str) -> None:
                    actual_identity = (
                        {**identity, "operation_id": "wrong-operation"}
                        if defect == "wrong-effect-identity"
                        else identity
                    )
                    spy.call(operation, actual_identity)
                    observed_operation, observed_identity = spy.calls[-1]
                    calls.append(
                        {
                            "operation": observed_operation,
                            "attempt": attempt,
                            "identity": observed_identity,
                        }
                    )

                for instruction in case["schedule"]:
                    if instruction.startswith("deliver:"):
                        if instruction not in seen or defect == "extra-effect":
                            effect("send")
                            seen.add(instruction)
                        state = "admitted"
                    elif instruction.startswith("sequence:"):
                        value = int(instruction.split(":")[1])
                        sequence = (
                            value
                            if defect == "regress-sequence"
                            else max(sequence, value)
                        )
                        state = "observed"
                    elif instruction == "send":
                        effect("send")
                        state = "sent"
                    elif instruction == "explicit-rejection":
                        state = "rejected"
                        if defect == "unauthorized-retry":
                            effect("send")
                    elif instruction == "retry-authorized":
                        self.assertEqual(state, "rejected")
                        attempt += 1
                        effect("send")
                        state = "sent"
                    elif instruction == "commit":
                        effect("commit")
                        state = "committed"
                    elif instruction == "stop":
                        state = "stopped"
                    elif instruction == "fresh-process-read":
                        state = "blocked"  # Model has no durable recovery adapter.
                    else:
                        state = "unknown"
                    steps.append(
                        {
                            "input": instruction,
                            "state": state,
                            "effect_calls": len(spy.calls),
                            "attempt": attempt,
                            "sequence": sequence,
                        }
                    )
                order_changed = steps != list(reversed(steps))
                if defect == "wrong-order":
                    steps.reverse()
                trace: dict[str, Any] = {
                    "case": case["id"],
                    "classification": "model_schedule_not_runtime_recovery",
                    "effect_calls": len(spy.calls),
                    "observation": labels[case["id"]],
                    "identity": identity,
                    "steps": steps,
                    "calls": calls,
                }
                relevant = (
                    (defect == "wrong-effect-identity" and bool(calls))
                    or (defect == "wrong-order" and order_changed)
                    or (
                        defect,
                        case["id"],
                    )
                    in {
                        ("extra-effect", "duplicate"),
                        ("regress-sequence", "reorder"),
                        ("unauthorized-retry", "retry"),
                    }
                )
                if relevant:
                    with self.assertRaises(kit.ContractError):
                        kit.assert_fault_trace(case["id"], trace)
                else:
                    self.assertTrue(kit.assert_fault_trace(case["id"], trace))
                    for field, malformed in (
                        ("effect_calls", True),
                        ("classification", "runtime_passed"),
                    ):
                        with self.assertRaises(kit.ContractError):
                            kit.assert_fault_trace(
                                case["id"], {**trace, field: malformed}
                            )
                    wrong = copy.deepcopy(trace)
                    wrong["identity"]["operation_id"] = "other-operation"
                    with self.assertRaises(kit.ContractError):
                        kit.assert_fault_trace(case["id"], wrong)
