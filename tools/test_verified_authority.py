#!/usr/bin/env python3
"""Fresh-process Temporal + SQLite offline authority with an independent peer ledger."""

from __future__ import annotations

import argparse
import base64
import concurrent.futures
import copy
import hashlib
import importlib.util
import json
import pathlib
import subprocess
import sys
import tempfile
import time
import uuid
from typing import Any, Callable

sys.path.insert(0, str(pathlib.Path(__file__).resolve().parents[1]))
from tools.runtime_artifact import validate

ROOT = pathlib.Path(__file__).resolve().parents[1]


def require(condition: Any, message: str) -> None:
    if not condition:
        raise AssertionError(message)


def test(platform: str, platform_prepared: str, core_prepared: str) -> dict[str, Any]:
    spec = importlib.util.spec_from_file_location(
        "authority_temporal_lane", pathlib.Path(platform) / "tools/temporal_lane.py"
    )
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    manifest = validate(ROOT, core_prepared)
    image = manifest["image_id"]
    checks = []
    with tempfile.TemporaryDirectory(prefix="runspindle-authority-") as tmp:
        directory = pathlib.Path(tmp)
        mounts = [{"source": tmp, "target": "/effects", "read_only": False}]
        scripts: list[dict[str, Any]] = []

        def save() -> None:
            p = directory / "script.next"
            p.write_text(json.dumps(scripts))
            p.replace(directory / "script.json")

        save()

        def events() -> list[dict[str, Any]]:
            p = directory / "calls.jsonl"
            return (
                [json.loads(line) for line in p.read_text().splitlines()]
                if p.exists()
                else []
            )

        def job(e: dict[str, Any]) -> str:
            return (
                "job-"
                + hashlib.sha256(
                    f"{e['binding']['task_id']}\n{e['binding']['revision']}".encode()
                ).hexdigest()
            )

        def wire(e: dict[str, Any]) -> str:
            return json.dumps(e, sort_keys=True, separators=(",", ":"))

        def envelope(name: str) -> dict[str, Any]:
            return {
                "version": "runspindle.envelope.v1",
                "message_type": "admission_request",
                "message_id": "message-" + name,
                "binding": {
                    "task_id": "task-" + name,
                    "revision": 1,
                    "operation_id": "operation-" + name,
                    "policy_revision": "policy-1",
                    "host_id": "host-1",
                    "workspace_id": "workspace-1",
                    "lease_epoch": 1,
                },
                "payload": {
                    "delivery_namespace": "synthetic",
                    "delivery_id": "delivery-" + name,
                    "predecessor_revision": None,
                    "normalized_digest": "a" * 64,
                    "grant_ref": "grant-" + name,
                },
            }

        def script(
            e: dict[str, Any], online: bool = False, **kw: Any
        ) -> dict[str, Any]:
            a = {"version": "runspindle.runtime.v1", **e["binding"], **e["payload"]}
            value = dict(
                admission=a,
                authorized=True,
                online=online,
                native_session_id="session-" + e["binding"]["task_id"],
                launch_lost_reply=False,
                crash_after_launch=False,
                terminal=None,
                cancel_acknowledged=True,
            )
            value.update(kw)
            scripts.append(value)
            save()
            return value

        def poll(predicate: Callable[[], Any], timeout: float = 40) -> Any:
            deadline = time.monotonic() + timeout
            while time.monotonic() < deadline:
                result = predicate()
                if result:
                    return result
                time.sleep(0.1)
            raise AssertionError("authority scenario deadline exceeded")

        with module.Lane(platform_prepared) as lane:

            def request(command: dict[str, Any], issuer: bool = False) -> Any:
                name = "command-" + uuid.uuid4().hex + ".json"
                (directory / name).write_text(json.dumps(command))
                args = (
                    ["synthetic-issuer", "/effects", "/effects/" + name]
                    if issuer
                    else ["client", "http://temporal:7233", "/effects/" + name]
                )
                return json.loads(
                    lane.run_client(image, args, mounts=mounts, deadline=40).stdout
                )

            def issue(e: dict[str, Any], **kwargs: Any) -> Any:
                return request(dict(command="issue", wire=wire(e), **kwargs), True)

            def admit(e: dict[str, Any]) -> Any:
                return request(
                    dict(
                        command="admit_authenticated",
                        registry_id="authenticated-v1",
                        wire=wire(e),
                    )
                )

            def registry() -> Any:
                return request(
                    dict(
                        command="authenticated_registry", registry_id="authenticated-v1"
                    )
                )

            def operation(e: dict[str, Any]) -> Any:
                return request(dict(command="authenticated_operation", job_id=job(e)))

            def cancel(e: dict[str, Any]) -> Any:
                return request(
                    dict(command="cancel_authenticated", job_id=job(e), wire=wire(e))
                )

            request({"command": "init"}, True)
            worker = lane.spawn_client(
                image,
                [
                    "authenticated-worker",
                    "http://temporal:7233",
                    "/effects",
                    "/effects/authority-helper.json",
                ],
                mounts=mounts,
            )
            request(
                {"command": "start_authenticated", "registry_id": "authenticated-v1"}
            )
            held = envelope("held")
            script(held)
            issue(held)
            with concurrent.futures.ThreadPoolExecutor(max_workers=2) as pool:
                replies = list(pool.map(lambda _: admit(held), range(2)))
            require(
                sorted(r["status"] for r in replies)
                == ["admitted", "duplicate_delivery"],
                "concurrent duplicate identity",
            )
            require(
                {r["job_id"] for r in replies} == {job(held)},
                "duplicate changed task identity",
            )
            poll(lambda: any(e["action"] == "observe_host" for e in events()))
            require(not list(directory.glob("effect-*.json")), "offline host launched")
            checks += [
                "concurrent_duplicate_one_retained_task_and_child",
                "offline_selected_host_zero_launch",
            ]
            invalid = []
            cases: list[dict[str, Any]] = [
                dict(bad_signature=True),
                dict(changes={"actor": "forged-actor"}),
                dict(changes={"audience": "wrong-core"}),
                dict(changes={"key_id": "unknown-key"}),
                dict(changes={"expires_at_ms": 1}),
                dict(changes={"not_before_ms": int(time.time() * 1000) + 600_000}),
                dict(changes={"actions": ["admit"]}),
                dict(changes={"policy_revision": "other-policy"}),
            ]
            for n, changes in enumerate(cases):
                e = envelope("denied-" + str(n))
                script(e, True)
                issue(e, **changes)
                # Admit-only is admitted but may never create an executable child.
                reply = admit(e)
                require(
                    reply["status"] == ("admitted" if n == 6 else "denied"),
                    "forged evidence admitted",
                )
                invalid.append(e)
            malformed = copy.deepcopy(held)
            malformed["version"] = "unsupported"
            require(
                admit(malformed)["status"] == "invalid", "unsupported envelope accepted"
            )
            changed = copy.deepcopy(held)
            changed["message_id"] = "altered-message"
            require(admit(changed)["status"] == "denied", "exact wire digest ignored")
            conflict = envelope("conflict")
            other = copy.deepcopy(conflict)
            other["message_id"] = "message-conflict2"
            other["payload"]["grant_ref"] = "grant-conflict2"
            other["payload"]["delivery_id"] = "delivery-conflict2"
            other["binding"]["operation_id"] = "other-operation"
            script(conflict)
            script(other)
            issue(conflict)
            issue(other)
            with concurrent.futures.ThreadPoolExecutor(max_workers=2) as pool:
                replies = list(pool.map(admit, [conflict, other]))
            require(
                sorted(r["status"] for r in replies) == ["admitted", "intent_conflict"],
                "concurrent conflicting revision mutated identity",
            )
            winner = [conflict, other][[r["status"] for r in replies].index("admitted")]
            retained = registry()["revisions"][job(winner)]
            require(
                retained
                == {
                    "version": "runspindle.runtime.v1",
                    **winner["binding"],
                    **winner["payload"],
                },
                "conflicting completion replaced the winning binding",
            )
            request(
                {"command": "revoke", "grant_ref": winner["payload"]["grant_ref"]}, True
            )
            pending = envelope("expired-pending")
            script(pending, True)
            pending_expires_ms = int(time.time() * 1000) + 30_000
            issue(pending, changes={"expires_at_ms": pending_expires_ms})
            require(
                admit(pending)["status"] == "admitted",
                "valid pending admission rejected",
            )
            while int(time.time() * 1000) <= pending_expires_ms:
                time.sleep(0.05)
            request(
                {"command": "revoke", "grant_ref": held["payload"]["grant_ref"]}, True
            )
            scripts[0]["online"] = True
            save()
            poll(lambda: job(pending) in registry()["terminal"])
            state = registry()
            require(len(state["revisions"]) == 4, "unauthorized registry mutation")
            require(
                all(s["native_session_id"] is None for s in state["terminal"].values()),
                "denied pending work invented native session",
            )
            require(
                not list(directory.glob("effect-*.json")),
                "expired/revoked pending launched",
            )
            require(
                not any(e["action"] == "launch" for e in events()),
                "forbidden launch adapter called",
            )
            checks += [
                "invalid_signature_unknown_key_actor_audience_policy_digest_time_denied",
                "concurrent_conflicting_revision_retains_one_identity",
                "expiry_and_revocation_before_dispatch_zero_adapter_calls",
                "admit_only_scope_zero_children",
            ]
            # Authorized cancellation has its own permission; acknowledgement is
            # never promoted to an external terminal outcome.
            active = envelope("cancel")
            active_script = script(active, True)
            issue(active)
            require(
                admit(active)["status"] == "admitted",
                "authorized cancel task admission",
            )
            poll(
                lambda: any(
                    e["action"] == "launch"
                    and e["admission"]["task_id"] == active["binding"]["task_id"]
                    for e in events()
                )
            )
            require(cancel(active)["accepted"], "authorized cancellation rejected")
            poll(lambda: operation(active)["cancellation"] == "acknowledged")
            require(
                operation(active)["state"] == "acknowledged",
                "cancel acknowledgement invented terminal outcome",
            )
            active_script["terminal"] = "completed"
            save()
            poll(lambda: job(active) in registry()["terminal"])
            no_cancel = envelope("no-cancel")
            ns = script(no_cancel, True)
            issue(no_cancel, changes={"actions": ["admit", "dispatch", "reconcile"]})
            require(admit(no_cancel)["status"] == "admitted", "scoped admission")
            poll(lambda: operation(no_cancel)["state"] == "acknowledged")
            require(
                not cancel(no_cancel)["accepted"],
                "missing cancel scope expanded authority",
            )
            ns["terminal"] = "completed"
            save()
            poll(lambda: job(no_cancel) in registry()["terminal"])
            checks += [
                "cancel_separate_scope_and_acknowledgement_not_terminal",
                "missing_cancel_action_zero_cancel_calls",
            ]
            recovery = envelope("recovery")
            script(recovery, True, launch_lost_reply=True, crash_after_launch=True)
            issue(recovery)
            require(admit(recovery)["status"] == "admitted", "recovery admission")
            receipt = directory / f"effect-{job(recovery)}.json"
            poll(receipt.exists)
            poll(
                lambda: subprocess.check_output(
                    ["docker", "inspect", worker, "--format", "{{.State.ExitCode}}"],
                    text=True,
                ).strip()
                == "93"
            )
            request(
                {"command": "revoke", "grant_ref": recovery["payload"]["grant_ref"]},
                True,
            )
            ledger = events()
            require(
                sum(e["action"] == "launch" for e in ledger) == 3,
                "unexpected launch count",
            )
            lane.restart(abrupt=True)
            worker2 = lane.spawn_client(
                image,
                [
                    "authenticated-worker",
                    "http://temporal:7233",
                    "/effects",
                    "/effects/authority-helper.json",
                ],
                mounts=mounts,
            )
            poll(lambda: operation(recovery)["state"] == "unknown")
            require(
                admit(recovery)["status"] == "denied",
                "replayed revoked admission restored authority",
            )
            require(not cancel(recovery)["accepted"], "revoked cancellation accepted")
            require(
                events() == ledger,
                "fresh worker invoked revoked adapter or retried uncertain launch",
            )
            require(
                json.loads(receipt.read_text())["native_session_id"]
                == "session-task-recovery",
                "external identity changed",
            )
            checks += [
                "fresh_temporal_sqlite_worker_recovery",
                "revocation_persists_across_processes",
                "unknown_outcome_retained_zero_blind_retry",
            ]
            # A missing/stale authoritative source always denies, even for a valid
            # script. Rename the DB only after readers are idle; test fresh clients.
            unavailable = envelope("unavailable")
            script(unavailable, True)
            issue(unavailable)
            db = directory / "authority.sqlite"
            db.rename(directory / "authority.unavailable")
            try:
                require(
                    admit(unavailable)["status"] == "denied",
                    "unavailable helper accepted",
                )
            finally:
                (directory / "authority.unavailable").rename(db)
            request({"command": "stale"}, True)
            require(admit(unavailable)["status"] == "denied", "stale helper accepted")
            require(events() == ledger, "unavailable/stale source invoked adapter")
            checks += ["missing_stale_authority_default_deny"]
            histories = {}
            for workflow in [
                "authenticated-v1",
                job(held),
                job(active),
                job(no_cancel),
                job(recovery),
            ]:
                history = request({"command": "history", "workflow_id": workflow})

                def inspect(value: Any) -> None:
                    if isinstance(value, dict):
                        if "payloads" in value:
                            for p in value["payloads"]:
                                content = base64.b64decode(p.get("data", ""))
                                require(
                                    all(
                                        x not in content
                                        for x in [
                                            b"synthetic-issuer",
                                            b"ephemeral.pk8",
                                            b"request_digest",
                                            b"signature",
                                        ]
                                    ),
                                    "assertion/key leaked into Temporal history",
                                )
                        for v in value.values():
                            inspect(v)
                    elif isinstance(value, list):
                        for v in value:
                            inspect(v)

                inspect(history)
                histories[workflow] = history
                path = directory / f"history-{workflow}.json"
                path.write_text(json.dumps(history))
                require(
                    json.loads(
                        lane.run_client(
                            image,
                            ["replay", "/effects/" + path.name],
                            mounts=mounts,
                            deadline=40,
                        ).stdout
                    )["status"]
                    == "passed",
                    "authenticated replay failed",
                )
            root = histories["authenticated-v1"]
            children = [
                e
                for e in root["events"]
                if "StartChildWorkflowExecutionInitiatedEventAttributes"
                in (e.get("attributes") or {})
            ]
            child_ids = [
                e["attributes"]["StartChildWorkflowExecutionInitiatedEventAttributes"][
                    "workflow_id"
                ]
                for e in children
            ]
            require(
                child_ids == [job(held), job(active), job(no_cancel), job(recovery)],
                "unauthorized child or duplicate identity in independent Temporal history",
            )
            require(events() == ledger, "replay invoked effect adapter")
            checks += [
                "real_history_child_id_inspection_zero_unauthorized_children",
                "authenticated_history_replay_no_effects_or_assertion_material",
            ]
            final = registry()
        for name in [lane.server, worker, worker2]:
            require(
                subprocess.run(
                    ["docker", "inspect", name], capture_output=True, timeout=30
                ).returncode
                != 0,
                "container leaked",
            )
        require(
            subprocess.run(
                ["docker", "volume", "inspect", lane.volume],
                capture_output=True,
                timeout=30,
            ).returncode
            != 0,
            "Temporal store leaked",
        )
        require(
            subprocess.run(
                ["docker", "network", "inspect", lane.network],
                capture_output=True,
                timeout=30,
            ).returncode
            != 0,
            "network leaked",
        )
        return {
            "status": "passed",
            "evidence_class": "isolated_integration",
            "checks": checks,
            "image_id": image,
            "binary_sha256": manifest["binary_sha256"],
            "source_hashes": manifest["source_hashes"],
            "retained_revisions": len(final["revisions"]),
            "child_ids": child_ids,
            "launch_count": 3,
            "cancel_count": sum(e["action"] == "cancel" for e in ledger),
            "history_digests": {
                k: hashlib.sha256(json.dumps(v, sort_keys=True).encode()).hexdigest()
                for k, v in histories.items()
            },
            "cleanup": "passed",
            "independent_review": "pending_exact_candidate_review",
            "boundary_matrix": {
                "core": "real",
                "Temporal": "real_disposable_SQLite",
                "authority": "real_ring_Ed25519_local_SQLite_ephemeral_synthetic_issuer",
                "host": "scripted_independent_fsynced_peer_ledger",
                "provider": "not_exercised",
                "browser": "not_exercised",
                "console": "not_exercised",
                "Tether": "not_exercised",
            },
            "live_acceptance": "not_run",
        }


if __name__ == "__main__":
    p = argparse.ArgumentParser()
    p.add_argument("--platform", required=True)
    p.add_argument("--platform-prepared", required=True)
    p.add_argument("--core-prepared", required=True)
    p.add_argument("--report", required=True)
    a = p.parse_args()
    report = test(a.platform, a.platform_prepared, a.core_prepared)
    pathlib.Path(a.report).write_text(json.dumps(report, indent=2) + "\n")
    print(json.dumps(report))
