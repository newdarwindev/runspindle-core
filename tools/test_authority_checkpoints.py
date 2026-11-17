#!/usr/bin/env python3
"""Real Temporal interruption at exact synthetic SDK boundaries, no live effects."""

from __future__ import annotations

import argparse
import base64
import concurrent.futures
import hashlib
import importlib.util
import json
import os
import pathlib
import signal
import subprocess
import sys
import tempfile
import uuid
from typing import Any

sys.path.insert(0, str(pathlib.Path(__file__).resolve().parents[1]))
from tools.runtime_artifact import validate
from tools.test_verified_authority import poll, require

ROOT = pathlib.Path(__file__).resolve().parents[1]


def run(
    platform: str,
    prepared: str,
    runtime: str,
    mode: str,
    point: str,
    revoked: bool,
    probe: str | None = None,
    probe_report: str | None = None,
) -> dict[str, Any]:
    manifest = validate(ROOT, runtime)
    image = manifest["image_id"]
    spec = importlib.util.spec_from_file_location(
        "checkpoint_lane", pathlib.Path(platform) / "tools/temporal_lane.py"
    )
    require(spec is not None and spec.loader is not None, "missing platform lane")
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)

    class Lane(module.Lane):  # type: ignore[name-defined,misc]
        def _limits(self, name: str, image: str, mounts: Any = ()) -> list[str]:
            args: list[str] = super()._limits(name, image, mounts)
            if any(m["target"] == "/effects" for m in mounts):
                args += ["--user", f"{os.getuid()}:{os.getgid()}"]
            return args

    with tempfile.TemporaryDirectory(prefix="authority-checkpoint-") as tmp:
        directory = pathlib.Path(tmp)
        mounts = [dict(source=tmp, target="/effects", read_only=False)]
        envelope: dict[str, Any] = dict(
            version="runspindle.envelope.v1",
            message_type="admission_request",
            message_id="message-checkpoint",
            binding=dict(
                task_id="task-checkpoint",
                revision=1,
                operation_id="operation-checkpoint",
                policy_revision="policy-1",
                host_id="host-1",
                workspace_id="workspace-1",
                lease_epoch=1,
            ),
            payload=dict(
                delivery_namespace="synthetic",
                delivery_id="delivery-checkpoint",
                predecessor_revision=None,
                normalized_digest="a" * 64,
                grant_ref="grant-checkpoint",
            ),
        )
        value: dict[str, Any] = envelope
        if mode == "scoped":
            value = dict(
                version="runspindle.scoped-admission.v1",
                request=envelope,
                destination=dict(
                    account_id="account-1",
                    destination_id="workspace-1.channel-1.thread-1",
                    delivery_operation_id="delivery-checkpoint",
                ),
            )
        elif mode == "intent":
            value = json.loads(
                (ROOT / "conformance/intent-v1/vectors.json").read_text()
            )[0]
            envelope = value["scoped"]["request"]
            source = value["event"]["source"]
            source.update(
                app_id="app-1",
                account_id="account-1",
                actor_id="synthetic-actor",
                workspace_id="workspace-1",
                channel_id="channel-1",
                thread_id="thread-1",
                message_id="message-1",
            )
            identity = [
                source[k]
                for k in (
                    "app_id",
                    "account_id",
                    "workspace_id",
                    "channel_id",
                    "thread_id",
                    "message_id",
                )
            ]
            envelope["binding"].update(
                task_id="tether-task:"
                + hashlib.sha256(
                    json.dumps(identity, separators=(",", ":")).encode()
                ).hexdigest(),
                host_id="host-1",
                workspace_id="workspace-1",
            )
            envelope["payload"]["normalized_digest"] = hashlib.sha256(
                json.dumps(
                    value["event"], sort_keys=True, separators=(",", ":")
                ).encode()
            ).hexdigest()
            value["scoped"]["destination"].update(
                account_id="account-1",
                destination_id="tether:"
                + hashlib.sha256(
                    json.dumps(identity[:-1], separators=(",", ":")).encode()
                ).hexdigest(),
            )
        wire = json.dumps(value, sort_keys=True, separators=(",", ":"))
        binding = envelope["binding"]
        job = "job-" + hashlib.sha256(f"{binding['task_id']}\n1".encode()).hexdigest()
        registry = "checkpoint-registry"
        if mode == "intent":
            source = value["event"]["source"]
            registry = (
                "intent-"
                + hashlib.sha256(
                    json.dumps(
                        [
                            source["app_id"],
                            source["account_id"],
                            source["workspace_id"],
                            binding["task_id"],
                        ],
                        separators=(",", ":"),
                    ).encode()
                ).hexdigest()
            )
        row = dict(
            admission=dict(
                version="runspindle.runtime.v1", **binding, **envelope["payload"]
            ),
            authorized=True,
            online=point == "before_dispatch_scheduling",
            native_session_id="checkpoint-session",
            launch_lost_reply=False,
            crash_after_launch=False,
            terminal=None,
            cancel_acknowledged=True,
        )

        def save() -> None:
            next_file = directory / "script.next"
            next_file.write_text(json.dumps([row]))
            next_file.replace(directory / "script.json")

        def effects() -> list[dict[str, Any]]:
            p = directory / "calls.jsonl"
            return (
                [json.loads(line) for line in p.read_text().splitlines()]
                if p.exists()
                else []
            )

        save()
        with Lane(prepared) as lane:

            def command(
                command: dict[str, Any],
                issuer: bool = False,
                client_checkpoint: bool = False,
            ) -> Any:
                name = uuid.uuid4().hex + ".json"
                (directory / name).write_text(json.dumps(command))
                args = (
                    ["synthetic-issuer", "/effects", "/effects/" + name]
                    if issuer
                    else ["client", "http://temporal:7233", "/effects/" + name]
                )
                if client_checkpoint:
                    args += ["before_reply"]
                return json.loads(
                    lane.run_client(image, args, mounts=mounts, deadline=100).stdout
                )

            def worker(checkpoint: bool) -> str:
                args = [
                    mode + "-worker",
                    "http://temporal:7233",
                    "/effects",
                    "/effects/authority-helper.json",
                ]
                if checkpoint and point != "before_reply":
                    args += [point, registry]
                return str(lane.spawn_client(image, args, mounts=mounts))

            command(
                dict(command="init_scoped" if mode in ("scoped", "intent") else "init"),
                True,
            )
            command(
                dict(
                    command={
                        "authenticated": "issue",
                        "scoped": "issue_scoped",
                        "intent": "issue_intent",
                    }[mode],
                    wire=wire,
                ),
                True,
            )
            old_worker = worker(True)
            if probe:
                require(probe_report is not None, "probe receipt required")
                assert probe_report is not None
                pathlib.Path(probe_report).write_text(
                    json.dumps(
                        dict(
                            server=lane.server,
                            worker=old_worker,
                            volume=lane.volume,
                            network=lane.network,
                            directory=tmp,
                        )
                    )
                )
                if probe == "cancel":
                    os.kill(os.getpid(), signal.SIGTERM)
                raise RuntimeError("deliberate checkpoint cleanup probe")
            command(
                dict(command="start_intent", wire=wire)
                if mode == "intent"
                else dict(command="start_" + mode, registry_id=registry)
            )
            admission = (
                dict(command="admit_intent", wire=wire)
                if mode == "intent"
                else dict(command="admit_" + mode, registry_id=registry, wire=wire)
            )
            original_status = "admitted"
            with concurrent.futures.ThreadPoolExecutor(max_workers=1) as pool:
                pending = pool.submit(
                    command, admission, False, point == "before_reply"
                )
                if point == "before_reply":
                    try:
                        pending.result(timeout=105)
                        raise AssertionError("client checkpoint did not lose reply")
                    except RuntimeError as error:
                        require(
                            str(error).startswith("client failed with exit 96:"),
                            "unexpected client failure",
                        )
                    subprocess.run(
                        ["docker", "kill", old_worker],
                        check=True,
                        capture_output=True,
                        timeout=20,
                    )
                else:
                    receipt = directory / "admission-checkpoint.json"
                    try:
                        poll(receipt.exists)
                    except AssertionError:
                        print(
                            "checkpoint timeout reply",
                            pending.result(timeout=105),
                            flush=True,
                        )
                        debug = command(dict(command="history", workflow_id=registry))
                        print(
                            "scheduled activities",
                            [
                                e["attributes"]["ActivityTaskScheduledEventAttributes"]
                                for e in debug["events"]
                                if "ActivityTaskScheduledEventAttributes"
                                in (e.get("attributes") or {})
                            ],
                            flush=True,
                        )
                        raise
                    observed = json.loads(receipt.read_text())
                    require(
                        observed["point"] == point
                        and observed["workflow_id"] == registry,
                        "wrong checkpoint scope",
                    )
                    state = poll(
                        lambda: (lambda s: s if not s["Running"] else None)(
                            json.loads(
                                subprocess.check_output(
                                    ["docker", "inspect", old_worker],
                                    text=True,
                                    timeout=20,
                                )
                            )[0]["State"]
                        )
                    )
                    require(state["ExitCode"] == 95, "checkpoint exit mismatch")
                before = command(dict(command="history", workflow_id=registry))
                durable_launches = 0
                for event in before["events"]:
                    scheduled = (event.get("attributes") or {}).get(
                        "ActivityTaskScheduledEventAttributes"
                    )
                    if scheduled:
                        for payload in (scheduled.get("input") or {}).get(
                            "payloads", []
                        ):
                            argument = json.loads(base64.b64decode(payload["data"]))
                            if (
                                isinstance(argument, dict)
                                and argument.get("call", {}).get("action") == "launch"
                            ):
                                durable_launches += 1
                if point == "before_dispatch_scheduling":
                    require(
                        durable_launches == 0,
                        "launch scheduling already durable at precommit checkpoint",
                    )
                before_digest = hashlib.sha256(
                    json.dumps(before, sort_keys=True).encode()
                ).hexdigest()
                before_children = sum(
                    "ChildWorkflowExecutionStartedEventAttributes"
                    in e.get("attributes", {})
                    for e in before["events"]
                )
                require(
                    not any(e["action"] == "launch" for e in effects()),
                    "launch preceded offline checkpoint",
                )
                if point in ("admission_commit", "before_child_creation"):
                    require(
                        before_children == 0, "pre-child boundary already created child"
                    )
                if point == "after_child_creation":
                    require(
                        before_children == 1, "post-child boundary lacks durable child"
                    )
                if revoked:
                    command(
                        dict(
                            command="revoke", grant_ref=envelope["payload"]["grant_ref"]
                        ),
                        True,
                    )
                row["online"] = True
                row["terminal"] = "completed"
                save()
                lane.restart(abrupt=True)
                new_worker = worker(False)
                if point != "before_reply":
                    reply = pending.result(timeout=105)
                    require(
                        reply["status"]
                        in (
                            ("admitted", "denied")
                            if revoked and point == "admission_commit"
                            else ("admitted",)
                        ),
                        "original accepted update did not recover",
                    )
                    original_status = reply["status"]
                retry = command(admission)
                require(
                    retry["status"]
                    == (
                        "denied"
                        if revoked
                        else ("duplicate" if mode == "intent" else "duplicate_delivery")
                    ),
                    "retry changed authority or identity",
                )
                if mode != "intent" and not revoked:
                    require(retry["job_id"] == job, "recovery changed stable job")

            def snapshot() -> Any:
                return command(
                    dict(command="intent", workflow_id=registry)
                    if mode == "intent"
                    else dict(command=mode + "_registry", registry_id=registry)
                )

            if original_status == "denied":
                final = snapshot()
                require(
                    not (
                        final["state"]["history"]
                        if mode == "intent"
                        else final["revisions"]
                    ),
                    "precommit denial retained unexpected revision",
                )
            elif mode == "intent" and revoked and point == "before_dispatch_scheduling":
                final = poll(
                    lambda: (
                        lambda s: s
                        if s["operation"] and s["operation"]["state"] == "unknown"
                        else None
                    )(snapshot())
                )
                require(
                    final["state"]["terminal_revision"] is None
                    and final["operation"]["native_session_id"] is None
                    and len(final["state"]["history"]) == 1,
                    "revoked scheduled operation invented terminal outcome",
                )
            elif mode == "intent":
                final = poll(
                    lambda: (
                        lambda s: s if s["state"]["terminal_revision"] == 1 else None
                    )(snapshot())
                )
                require(
                    len(final["state"]["history"]) == 1, "duplicate semantic revision"
                )
            else:
                final = poll(
                    lambda: (lambda s: s if job in s["terminal"] else None)(snapshot())
                )
                require(len(final["revisions"]) == 1, "duplicate registry revision")
            ledger = effects()
            launches = [e for e in ledger if e["action"] == "launch"]
            require(
                len(launches) == (0 if revoked else 1),
                "duplicate or unauthorized launch",
            )
            require(
                all(
                    all(e["admission"][k] == v for k, v in binding.items())
                    for e in ledger
                ),
                "alternate target or lease observed",
            )
            history = command(dict(command="history", workflow_id=registry))
            children = [
                e["attributes"]["ChildWorkflowExecutionStartedEventAttributes"][
                    "workflow_execution"
                ]["workflowId"]
                for e in history["events"]
                if "ChildWorkflowExecutionStartedEventAttributes"
                in e.get("attributes", {})
            ]
            require(
                len(children) <= 1 and all(c == job for c in children),
                "duplicate/alternate child",
            )
            if mode != "intent" and not revoked:
                require(children == [job], "missing recovered child")
            histories = {}
            for identifier in [registry, *children]:
                history = command(dict(command="history", workflow_id=identifier))
                path = directory / "history.json"
                path.write_text(json.dumps(history))
                histories[identifier] = hashlib.sha256(path.read_bytes()).hexdigest()
                replay = lane.run_client(
                    image,
                    ["replay", "/effects/history.json"],
                    mounts=mounts,
                    deadline=60,
                )
                require(
                    json.loads(replay.stdout)["status"] == "passed", "replay failed"
                )
            require(effects() == ledger, "replay caused effect")
        # Independent absence confirmation: a daemon/permission error is not absence.
        for kind, names in [
            ("container", [old_worker, new_worker, lane.server]),
            ("network", [lane.network]),
            ("volume", [lane.volume]),
        ]:
            listing = subprocess.check_output(
                ["docker", kind, "ls", "-q", "--no-trunc"]
                if kind == "container"
                else ["docker", kind, "ls", "--format", "{{.Name}}"],
                text=True,
                timeout=30,
            )
            if kind == "container":
                listing = subprocess.check_output(
                    ["docker", "ps", "-a", "--format", "{{.Names}}"],
                    text=True,
                    timeout=30,
                )
            require(
                all(name not in listing.splitlines() for name in names),
                "owned resource leaked",
            )
    require(not directory.exists(), "temporary state leaked")
    return dict(
        status="passed",
        evidence_class="isolated_integration",
        mode=mode,
        point=point,
        revoked_during_downtime=revoked,
        before_children=before_children,
        before_durable_launch_commands=durable_launches,
        after_children=len(children),
        launch_count=len(launches),
        original_admission_status=original_status,
        semantic_operation_state=(
            final["operation"]["state"]
            if mode == "intent" and final["operation"]
            else None
        ),
        semantic_terminal_revision=(
            final["state"]["terminal_revision"] if mode == "intent" else None
        ),
        retained_revisions=len(
            final["state"]["history"] if mode == "intent" else final["revisions"]
        ),
        before_history_sha256=before_digest,
        history_digests=histories,
        image_id=image,
        binary_sha256=manifest["binary_sha256"],
        source_hashes=manifest["source_hashes"],
        cleanup="passed",
        live_acceptance="not_run",
    )


if __name__ == "__main__":
    p = argparse.ArgumentParser()
    for flag in (
        "platform",
        "platform-prepared",
        "prepared",
        "mode",
        "point",
        "report",
    ):
        p.add_argument("--" + flag, required=True)
    p.add_argument("--revoke", action="store_true")
    p.add_argument("--probe", choices=["failure", "cancel"])
    p.add_argument("--probe-report")
    a = p.parse_args()
    require(a.mode in ("authenticated", "scoped", "intent"), "unknown mode")

    def interrupted(_signum: int, _frame: Any) -> None:
        raise KeyboardInterrupt("checkpoint qualification interrupted")

    signal.signal(signal.SIGTERM, interrupted)
    report = run(
        a.platform,
        a.platform_prepared,
        a.prepared,
        a.mode,
        a.point,
        a.revoke,
        a.probe,
        a.probe_report,
    )
    pathlib.Path(a.report).write_text(json.dumps(report, indent=2) + "\n")
    print(json.dumps(report))
