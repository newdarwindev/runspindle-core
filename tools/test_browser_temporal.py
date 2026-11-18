#!/usr/bin/env python3
"""Actual Temporal/worker recovery over Core's synthetic Browser activity peer.

Not an actual browser, production authority, or power-loss qualification.
"""

from __future__ import annotations

import argparse
import copy
import hashlib
import importlib.util
import json
import pathlib
import sqlite3
import subprocess
import sys
import tempfile
import time
import uuid
from typing import Any

ROOT = pathlib.Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
from tools.runtime_artifact import validate  # noqa: E402


def check(value: Any, message: str) -> None:
    if not value:
        raise AssertionError(message)


def test(
    platform: str,
    platform_prepared: str,
    core_prepared: str,
    browser_source: str | None = None,
) -> dict[str, Any]:
    path = pathlib.Path(platform) / "tools/temporal_lane.py"
    spec = importlib.util.spec_from_file_location("browser_temporal_lane", path)
    if spec is None or spec.loader is None:
        raise RuntimeError("platform lane unavailable")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    manifest = validate(ROOT, core_prepared)
    image = manifest["image_id"]
    checks: list[str] = []
    cases: dict[str, Any] = {}
    for mode in ("after", "before"):
        with tempfile.TemporaryDirectory(prefix="core-browser-temporal-") as tmp:
            directory = pathlib.Path(tmp)
            target: dict[str, Any] = {
                "binding": {
                    "principal": "principal",
                    "account": "account",
                    "browser": "browser",
                    "tab": "tab",
                    "intent": "intent",
                    "revision": 1,
                    "operation": "operation",
                    "epoch": 1,
                    "origin": "https://example.invalid",
                },
                "native_session": "session",
                "frame": "frame",
                "frame_origin": "https://example.invalid",
            }
            payload = b"synthetic-browser-effect"
            (directory / "browser-payload.bin").write_bytes(payload)
            script = {
                "target": target,
                "expected_sha256": hashlib.sha256(payload).hexdigest(),
                "dispatch_authorized": True,
                "reconcile_authorized": True,
                "not_after_unix_ms": int(time.time() * 1000) + 300_000,
                "crash_after_effect": mode == "after",
                "crash_before_effect": mode == "before",
                "lost_reply": True,
            }

            peer_root = directory / "browser-peer"
            if browser_source is not None:
                peer_root.mkdir()
                (peer_root / "target.json").write_text(json.dumps(target))
                (peer_root / "payload.bin").write_bytes(payload)
                (peer_root / "crash-core-after-exchange").touch()
                if mode == "before":
                    (peer_root / "crash-before-site").touch()

            def install() -> None:
                pending = directory / "browser-script.next"
                pending.write_text(json.dumps(script))
                pending.replace(directory / "browser-script.json")

            def rows(table: str) -> list[Any]:
                db = (
                    directory / "browser-site.sqlite"
                    if browser_source is None
                    else peer_root / "site.sqlite"
                )
                if not db.exists():
                    return []
                with sqlite3.connect(f"file:{db}?mode=ro", uri=True) as connection:
                    return connection.execute(f"SELECT * FROM {table}").fetchall()

            def poll(predicate: Any, timeout: float = 30) -> Any:
                deadline = time.monotonic() + timeout
                last = None
                while time.monotonic() < deadline:
                    last = predicate()
                    if last:
                        return last
                    time.sleep(0.15)
                raise AssertionError(f"{mode}: poll deadline; last={last!r}")

            install()
            mounts = [
                {"source": str(directory), "target": "/effects", "read_only": False}
            ]
            worker_mode = "browser-worker"
            if browser_source is not None:
                worker_mode = "browser-worker-pinned-fixture"
                mounts.extend(
                    [
                        {
                            "source": str(pathlib.Path(browser_source).resolve()),
                            "target": "/browser-source",
                            "read_only": True,
                        },
                        {
                            "source": str(ROOT / "tests/fixtures"),
                            "target": "/browser-fixture",
                            "read_only": True,
                        },
                    ]
                )
            with module.Lane(platform_prepared) as lane:

                def client(command: dict[str, Any]) -> Any:
                    name = f"command-{uuid.uuid4().hex}.json"
                    (directory / name).write_text(json.dumps(command))
                    result = lane.run_client(
                        image,
                        ["client", "http://temporal:7233", f"/effects/{name}"],
                        mounts=mounts,
                        deadline=25,
                    )
                    return json.loads(result.stdout)

                worker = lane.spawn_client(
                    image,
                    [worker_mode, "http://temporal:7233", "/effects"],
                    mounts=mounts,
                )
                started = client({"command": "start_browser", "target": target})
                workflow_id = started["workflow_id"]

                def duplicate_rejected() -> None:
                    try:
                        client({"command": "start_browser", "target": target})
                    except RuntimeError as error:
                        check(
                            "already" in str(error).lower(),
                            "duplicate start failed for an unrelated reason",
                        )
                    else:
                        raise AssertionError("duplicate workflow start accepted")

                duplicate_rejected()

                def snapshot() -> Any:
                    return client({"command": "browser", "workflow_id": workflow_id})

                poll(
                    lambda: subprocess.check_output(
                        ["docker", "inspect", worker, "--format", "{{.State.Running}}"],
                        text=True,
                        timeout=10,
                    ).strip()
                    == "false"
                )
                exit_code = int(
                    subprocess.check_output(
                        [
                            "docker",
                            "inspect",
                            worker,
                            "--format",
                            "{{.State.ExitCode}}",
                        ],
                        text=True,
                        timeout=10,
                    )
                )
                check(
                    exit_code
                    == (95 if browser_source is not None or mode == "after" else 94),
                    "unexpected crash exit",
                )
                check(
                    len(rows("effects")) == (1 if mode == "after" else 0),
                    "independent site effect count before recovery",
                )
                if browser_source is not None:
                    check(
                        rows("exchanges") == [("dispatch",)],
                        "actual Browser dispatch exchange count",
                    )
                    check(
                        (peer_root / "journal.sqlite").is_file(),
                        "actual Browser journal missing",
                    )
                    (peer_root / "crash-core-after-exchange").unlink()
                initial_calls = rows("calls")
                check(len(initial_calls) == 1, "expected one dispatch peer call")
                script.update(
                    crash_after_effect=False,
                    crash_before_effect=False,
                    reconcile_authorized=False,
                )
                install()
                lane.restart(abrupt=True)
                worker2 = lane.spawn_client(
                    image,
                    [worker_mode, "http://temporal:7233", "/effects"],
                    mounts=mounts,
                )
                recovered = poll(
                    lambda: (
                        value if (value := snapshot())["dispatch_finished"] else None
                    )
                )
                check(
                    recovered["historical"]["outcome"] == "unknown",
                    "restart invented outcome",
                )
                check(
                    rows("calls") == initial_calls,
                    "retry re-entered peer after reservation",
                )
                wrong = copy.deepcopy(target)
                wrong["binding"]["epoch"] = 2
                check(
                    not client(
                        {
                            "command": "reconcile_browser",
                            "workflow_id": workflow_id,
                            "target": wrong,
                        }
                    )["accepted"],
                    "changed target accepted",
                )
                check(
                    client(
                        {
                            "command": "reconcile_browser",
                            "workflow_id": workflow_id,
                            "target": target,
                        }
                    )["accepted"],
                    "explicit reconcile not queued",
                )
                denied = poll(
                    lambda: (
                        value
                        if (value := snapshot())["completed_reconciles"] == 1
                        else None
                    )
                )
                check(denied["historical"] is None, "revoked grant returned success")
                check(rows("calls") == initial_calls, "revoked reconcile invoked peer")
                script["reconcile_authorized"] = True
                install()
                final_reconciles = 2
                if browser_source is not None:
                    check(
                        rows("exchanges") == [("dispatch",)],
                        "Core denial entered Browser",
                    )
                    (peer_root / "revoked").touch()
                    check(
                        client(
                            {
                                "command": "reconcile_browser",
                                "workflow_id": workflow_id,
                                "target": target,
                            }
                        )["accepted"],
                        "Browser denied reconcile not queued",
                    )
                    browser_denied = poll(
                        lambda: (
                            value
                            if (value := snapshot())["completed_reconciles"] == 2
                            else None
                        )
                    )
                    check(
                        browser_denied["historical"]["outcome"] == "unknown",
                        "revoked actual Browser authority returned confirmation",
                    )
                    check(
                        rows("calls") == initial_calls,
                        "revoked Browser authority invoked site",
                    )
                    check(
                        rows("exchanges") == [("dispatch",), ("reconcile",)],
                        "actual Browser authority denial not exercised",
                    )
                    (peer_root / "revoked").unlink()
                    final_reconciles = 3
                check(
                    client(
                        {
                            "command": "reconcile_browser",
                            "workflow_id": workflow_id,
                            "target": target,
                        }
                    )["accepted"],
                    "fresh reconcile not queued",
                )
                final = poll(
                    lambda: (
                        value
                        if (value := snapshot())["completed_reconciles"]
                        == final_reconciles
                        else None
                    )
                )
                check(
                    final["historical"]["outcome"]
                    == ("confirmed" if mode == "after" else "unknown"),
                    "reconciliation did not respect independent effect",
                )
                check(len(rows("calls")) == 2, "reconciliation resubmitted dispatch")
                check(
                    len(rows("effects")) == (1 if mode == "after" else 0),
                    "recovery changed independent site effect count",
                )
                ledger = rows("calls")
                check(
                    [row[0] for row in ledger]
                    == (
                        ["dispatch", "reconcile"]
                        if browser_source is None
                        else ["submit", "readback"]
                    ),
                    "unexpected peer call order or action",
                )
                if mode == "after":
                    if browser_source is None:
                        operation, stored_target, stored_payload, stored_digest = rows(
                            "effects"
                        )[0]
                    else:
                        operation, stored_payload, stored_target = rows("effects")[0]
                        stored_digest = hashlib.sha256(stored_payload).hexdigest()
                    check(
                        operation == target["binding"]["operation"]
                        and json.loads(stored_target) == target
                        and stored_payload == payload
                        and stored_digest == hashlib.sha256(payload).hexdigest(),
                        "independent site retained different target or bytes",
                    )
                    poll(
                        lambda: any(
                            "WorkflowExecutionCompletedEventAttributes"
                            in event.get("attributes", {})
                            for event in client(
                                {"command": "history", "workflow_id": workflow_id}
                            )["events"]
                        )
                    )
                    duplicate_rejected()
                exchanges = rows("exchanges") if browser_source is not None else []
                if browser_source is not None:
                    check(
                        exchanges == [("dispatch",), ("reconcile",), ("reconcile",)],
                        "actual Browser exchange count changed",
                    )
                history = client({"command": "history", "workflow_id": workflow_id})
                history_path = directory / "history.json"
                history_path.write_text(json.dumps(history))
                replay = lane.run_client(
                    image,
                    ["replay", "/effects/history.json"],
                    mounts=mounts,
                    deadline=40,
                )
                check(
                    json.loads(replay.stdout)["status"] == "passed",
                    "real history replay failed",
                )
                check(rows("calls") == ledger, "history replay invoked peer")
                if browser_source is not None:
                    check(
                        rows("exchanges") == exchanges, "replay entered actual Browser"
                    )
                cases[mode] = {
                    "exit_code": exit_code,
                    "peer_calls": len(ledger),
                    "site_effects": len(rows("effects")),
                    "final": final,
                    "browser_exchanges": len(exchanges),
                    "history_sha256": hashlib.sha256(
                        history_path.read_bytes()
                    ).hexdigest(),
                }
            for name in (lane.server, worker, worker2):
                check(
                    subprocess.run(
                        ["docker", "inspect", name], capture_output=True, timeout=15
                    ).returncode
                    != 0,
                    "lane leaked container",
                )
            checks.extend(
                [
                    f"{mode}_fresh_worker_and_server_recovery",
                    f"{mode}_reservation_stops_retry",
                    f"{mode}_fresh_authority_required",
                    f"{mode}_exact_target_required",
                    f"{mode}_explicit_reconcile_only",
                    f"{mode}_actual_history_replay_zero_peer_calls",
                ]
            )
    return {
        "status": "passed",
        "evidence_class": "isolated_integration",
        "checks": checks,
        "cases": cases,
        "image_id": image,
        "source_hashes": manifest["source_hashes"],
        "boundary_matrix": {
            "core": "real_registry_and_sdk",
            "temporal": "real_dev_database",
            "browser": (
                "scripted_peer"
                if browser_source is None
                else "pinned_real_guarded_journal_receiver"
            ),
            "site": "independent_synthetic_sqlite",
            "authority": "explicit_synthetic_script",
            "provider": "not_exercised",
        },
        "live_acceptance": "not_run",
        "cleanup": "passed",
    }


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--platform", required=True)
    parser.add_argument("--platform-prepared", required=True)
    parser.add_argument("--core-prepared", required=True)
    parser.add_argument("--report", required=True)
    parser.add_argument("--browser-source")
    args = parser.parse_args()
    result = test(
        args.platform, args.platform_prepared, args.core_prepared, args.browser_source
    )
    pathlib.Path(args.report).write_text(json.dumps(result, indent=2) + "\n")
    print(json.dumps(result))
