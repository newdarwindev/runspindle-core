#!/usr/bin/env python3
"""Bounded real Core/Temporal consumer + real Tether codec, synthetic authority/host."""

from __future__ import annotations

import argparse
import base64
import hashlib
import importlib.util
import json
import os
import pathlib
import signal
import subprocess
import sys
import tempfile
import time
import uuid
from typing import Any, cast

sys.path.insert(0, str(pathlib.Path(__file__).resolve().parents[1]))
from tools.runtime_artifact import validate

ROOT = pathlib.Path(__file__).resolve().parents[1]


def require(ok: Any, message: str) -> None:
    if not ok:
        raise AssertionError(message)


def test(
    platform: str,
    platform_prepared: str,
    prepared: str,
    tether: str,
    probe: str | None = None,
    probe_report: str | None = None,
    prior_core: str | None = None,
    prior_prepared: str | None = None,
) -> dict[str, Any]:
    sys.path.insert(0, tether)
    from tether.admission import canonical  # type: ignore[import-not-found]
    from tether.intent import build_intent  # type: ignore[import-not-found]

    spec = importlib.util.spec_from_file_location(
        "intent_lane", pathlib.Path(platform) / "tools/temporal_lane.py"
    )
    if spec is None or spec.loader is None:
        raise RuntimeError("missing pinned lane")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    manifest = validate(ROOT, prepared)
    image = manifest["image_id"]
    require(
        bool(prior_core) == bool(prior_prepared),
        "both prior source and prepared required",
    )
    prior = (
        validate(pathlib.Path(prior_core), prior_prepared)
        if prior_core and prior_prepared
        else None
    )
    require(
        prior is None or prior["binary_sha256"] != manifest["binary_sha256"],
        "upgrade requires distinct old executable",
    )
    prior_histories: dict[str, str] = {}
    checks = []

    class Lane(module.Lane):  # type: ignore[name-defined,misc]
        def _limits(self, name: str, image: str, mounts: Any = ()) -> list[str]:
            args: list[str] = super()._limits(name, image, mounts)
            if any(m["target"] == "/effects" for m in mounts):
                args += ["--user", f"{os.getuid()}:{os.getgid()}"]
            return args

    with tempfile.TemporaryDirectory(prefix="runspindle-intent-") as tmp:
        directory = pathlib.Path(tmp)
        mounts = [dict(source=tmp, target="/effects", read_only=False)]
        scripts: list[dict[str, Any]] = []
        ids: list[str] = []

        def save() -> None:
            p = directory / "script.next"
            p.write_text(json.dumps(scripts))
            p.replace(directory / "script.json")

        save()

        def events() -> list[dict[str, Any]]:
            p = directory / "calls.jsonl"
            return (
                [json.loads(x) for x in p.read_text().splitlines()]
                if p.exists()
                else []
            )

        def poll(fn: Any) -> Any:
            until = time.monotonic() + 50
            while time.monotonic() < until:
                value = fn()
                if value:
                    return value
                time.sleep(0.1)
            print("observed_events", events(), flush=True)
            print("snapshots", {name: snap(name) for name in ids}, flush=True)
            logs = subprocess.run(
                ["docker", "logs", "--tail", "30", worker],
                capture_output=True,
                text=True,
                timeout=10,
            )
            print("worker_log", logs.stdout, logs.stderr, flush=True)
            raise AssertionError("intent scenario deadline")

        def make(
            name: str,
            revision: int = 1,
            kind: str = "create",
            previous: bytes | None = None,
        ) -> bytes:
            event = dict(
                version="runspindle.tether.ingress.v1",
                event_id=f"event-{name}-{revision}",
                message_id=f"message-{name}",
                actor_id="synthetic-actor",
                workspace_id="workspace-1",
                channel_id="channel-1",
                thread_id="thread-1",
                revision=revision,
                kind=kind,
                text="" if kind == "delete" else f"synthetic-{name}-{revision}",
                mirrored=False,
            )
            result = build_intent(
                canonical(event),
                app_id="app-1",
                account_id="account-1",
                binding=dict(
                    task_id="tether-task:"
                    + hashlib.sha256(
                        canonical(
                            [
                                "app-1",
                                "account-1",
                                "workspace-1",
                                "channel-1",
                                "thread-1",
                                f"message-{name}",
                            ]
                        )
                    ).hexdigest(),
                    revision=revision,
                    operation_id=f"op-{name}-{revision}",
                    policy_revision="policy-1",
                    host_id="host-1",
                    workspace_id="workspace-1",
                    lease_epoch=1,
                ),
                delivery_namespace="intent-test",
                delivery_operation_id=f"delivery-{name}-{revision}",
                grant_ref=f"grant-{name}-{revision}",
                previous_wire=previous,
            )
            return cast(bytes, result)

        def admission(wire: bytes) -> dict[str, Any]:
            e = json.loads(wire)["scoped"]["request"]
            return dict(version="runspindle.runtime.v1", **e["binding"], **e["payload"])

        def job(wire: bytes) -> str:
            a = admission(wire)
            return (
                "job-"
                + hashlib.sha256(
                    f"{a['task_id']}\n{a['revision']}".encode()
                ).hexdigest()
            )

        def script(wire: bytes, **kw: Any) -> dict[str, Any]:
            row = dict(
                admission=admission(wire),
                authorized=True,
                online=True,
                native_session_id="session-" + job(wire),
                launch_lost_reply=False,
                crash_after_launch=False,
                terminal=None,
                cancel_acknowledged=True,
            )
            row.update(kw)
            scripts.append(row)
            save()
            return row

        with Lane(platform_prepared) as lane:

            def request(command: dict[str, Any], issuer: bool = False) -> Any:
                name = "command-" + uuid.uuid4().hex + ".json"
                (directory / name).write_text(json.dumps(command))
                args = (
                    ["synthetic-issuer", "/effects", "/effects/" + name]
                    if issuer
                    else ["client", "http://temporal:7233", "/effects/" + name]
                )
                print("command", command["command"], flush=True)
                try:
                    return json.loads(
                        lane.run_client(image, args, mounts=mounts, deadline=45).stdout
                    )
                except subprocess.TimeoutExpired:
                    print(
                        subprocess.run(
                            ["docker", "logs", "--tail", "40", worker],
                            capture_output=True,
                            text=True,
                            timeout=10,
                        ).stdout,
                        flush=True,
                    )
                    raise

            def issue(w: bytes, **kw: Any) -> Any:
                return request(
                    dict(command="issue_intent", wire=w.decode(), **kw), True
                )

            def start(w: bytes) -> str:
                id = request(dict(command="start_intent", wire=w.decode()))[
                    "workflow_id"
                ]
                ids.append(id)
                return cast(str, id)

            def admit(w: bytes) -> str:
                return cast(
                    str,
                    request(dict(command="admit_intent", wire=w.decode()))["status"],
                )

            def snap(id: str) -> Any:
                return request(dict(command="intent", workflow_id=id))

            request(dict(command="init"), True)
            worker = lane.spawn_client(
                prior["image_id"] if prior else image,
                [
                    "intent-worker",
                    "http://temporal:7233",
                    "/effects",
                    "/effects/authority-helper.json",
                ],
                mounts=mounts,
            )
            if probe is not None:
                require(probe_report is not None, "probe receipt required")
                pathlib.Path(cast(str, probe_report)).write_text(
                    json.dumps(
                        dict(
                            server=lane.server,
                            worker=worker,
                            volume=lane.volume,
                            network=lane.network,
                            directory=tmp,
                        )
                    )
                )
                if probe == "cancel":
                    os.kill(os.getpid(), signal.SIGTERM)
                raise RuntimeError("intent deliberate cleanup failure probe")
            # Wrong signatures create no accepted revision or adapter effect.
            bad = make("forged")
            bad_id = start(bad)
            issue(bad, bad_signature=True)
            require(admit(bad) == "denied", "forged admission")
            require(
                snap(bad_id)["state"]["history"] == [] and events() == [],
                "forbidden durable/effect state",
            )
            checks.append("forged_authority_zero_retained_revision_or_effect")
            # Pre-launch deletion while host activity is pending prevents scheduling launch.
            first = make("before")
            deleted = make("before", 2, "delete", first)
            script(first)
            (directory / f"intent-observe-hold-{job(first)}").touch()
            issue(first)
            issue(deleted)
            id = start(first)
            require(admit(first) == "admitted", "initial admission")
            poll(lambda: any(e["action"] == "observe_host" for e in events()))
            require(admit(deleted) == "admitted", "delete admission")
            (directory / f"intent-observe-release-{job(first)}.json").write_text(
                json.dumps(json.loads(first)["scoped"]["request"]["binding"])
            )
            poll(lambda: snap(id)["operation"]["state"] == "observed_cancelled")
            require(
                not any(e["action"] == "launch" for e in events()),
                "prelaunch delete launched",
            )
            require(admit(deleted) == "duplicate", "delete replay")
            checks.append("delete_during_host_verification_zero_launch")
            # Active cancellation is authorized by the NEW delete grant after revocation.
            first = make("active")
            deleted = make("active", 2, "delete", first)
            row = script(first)
            issue(first)
            issue(deleted)
            id = start(first)
            require(admit(first) == "admitted", "active admission")
            poll(lambda: snap(id)["operation"]["native_session_id"] is not None)
            edit = make("active", 2, "edit", first)
            # Same grant reference cannot be issued for another wire; sign separately.
            edit_obj = json.loads(edit)
            edit_obj["scoped"]["request"]["payload"]["grant_ref"] = "grant-active-edit"
            edit = canonical(edit_obj)
            issue(edit)
            require(admit(edit) == "predecessor_active", "active edit not rejected")
            request(dict(command="revoke", grant_ref="grant-active-1"), True)
            require(admit(deleted) == "admitted", "fresh delete authority")
            poll(lambda: snap(id)["operation"]["cancellation"] == "acknowledged")
            require(
                snap(id)["operation"]["state"]
                not in ("observed_cancelled", "observed_completed"),
                "ack forged terminal",
            )
            row["terminal"] = "cancelled"
            save()
            poll(lambda: snap(id)["operation"]["state"] == "observed_cancelled")
            checks.append(
                "current_delete_authority_cancels_revoked_prior_ack_not_terminal"
            )
            # A launch already scheduled before delete is not claimed undone.
            first = make("scheduled")
            deleted = make("scheduled", 2, "delete", first)
            row = script(first)
            issue(first)
            issue(deleted)
            id = start(first)
            (directory / f"intent-launch-hold-{job(first)}").touch()
            require(admit(first) == "admitted", "scheduled admission")

            # Payloads are base64: independent history parser confirms the scheduled call.
            def scheduled() -> bool:
                h = request(dict(command="history", workflow_id=id))
                for e in h.get("events", []):
                    attrs = (e.get("attributes") or {}).get(
                        "ActivityTaskScheduledEventAttributes", {}
                    )
                    for p in (attrs.get("input") or {}).get("payloads", []):
                        try:
                            obj = json.loads(base64.b64decode(p.get("data", "")))
                            if obj.get("call", {}).get("action") == "launch":
                                return True
                        except (ValueError, TypeError):
                            pass
                return False

            poll(scheduled)
            require(admit(deleted) == "admitted", "scheduled delete")
            require(
                snap(id)["operation"]["cancellation"] == "requested",
                "pending cancellation lost",
            )
            (directory / f"intent-launch-release-{job(first)}.json").write_text(
                json.dumps(json.loads(first)["scoped"]["request"]["binding"])
            )
            poll(lambda: snap(id)["operation"]["cancellation"] == "acknowledged")
            row["terminal"] = "cancelled"
            save()
            poll(lambda: snap(id)["operation"]["state"] == "observed_cancelled")
            checks.append(
                "already_scheduled_launch_reconciles_then_cancels_no_atomic_fence_claim"
            )
            # Complete chain and fresh-process persistence/replay.
            first = make("chain")
            second = make("chain", 2, "edit", first)
            deleted = make("chain", 3, "delete", second)
            script(first, terminal="completed")
            script(second, terminal="completed")
            for w in [first, second, deleted]:
                issue(w)
            id = start(first)
            require(admit(second) == "missing_predecessor", "out-of-order edit")
            require(admit(first) == "admitted", "chain create")
            poll(lambda: snap(id)["state"]["terminal_revision"] == 1)
            require(admit(second) == "admitted", "chain edit")
            poll(lambda: snap(id)["state"]["terminal_revision"] == 2)
            require(admit(deleted) == "admitted", "chain delete")

            def batch_wire(wires: list[bytes]) -> bytes:
                return cast(
                    bytes,
                    canonical(
                        dict(
                            version="runspindle.intent-batch.v1",
                            intents=[json.loads(w) for w in wires],
                        )
                    ),
                )

            def submit_batch(wires: list[bytes]) -> Any:
                return request(
                    dict(command="admit_intent_batch", wire=batch_wire(wires).decode())
                )

            def receipt(wires: list[bytes]) -> Any:
                return request(
                    dict(command="intent_receipt", wire=batch_wire(wires).decode())
                )

            # A complete latest tombstone arrives before any Core admission.
            c = make("batch_deleted")
            e = make("batch_deleted", 2, "edit", c)
            d = make("batch_deleted", 3, "delete", e)
            issue(d)
            id = start(c)
            before_effects = events()
            require(
                submit_batch([c, e, d])["status"] == "admitted", "atomic delete batch"
            )
            state = snap(id)["state"]
            require(
                state["tombstone"]
                and state["superseded_without_execution"] == [1, 2]
                and state["pending"] is None,
                "stale batch work scheduled",
            )
            require(
                events() == before_effects, "deleted batch executed stale intermediate"
            )
            require(receipt([c, e, d])["status"] == "retained", "lost ack readback")
            require(
                submit_batch([c, e, d])["status"] == "duplicate", "batch ack replay"
            )
            # Last edit is current, missing intermediates are metadata, not work.
            c = make("batch_edit")
            e = make("batch_edit", 2, "edit", c)
            row = script(e, terminal="completed")
            issue(e)
            id = start(c)
            require(submit_batch([c, e])["status"] == "admitted", "latest edit batch")
            poll(lambda: snap(id)["state"]["terminal_revision"] == 2)
            require(
                snap(id)["state"]["superseded_without_execution"] == [1],
                "prior mislabeled terminal",
            )
            # Missing local edit cannot strand deletion of the still active ancestor.
            c = make("batch_active")
            e = make("batch_active", 2, "edit", c)
            d = make("batch_active", 3, "delete", e)
            row = script(c)
            issue(c)
            issue(d)
            id = start(c)
            require(admit(c) == "admitted", "batch active create")
            poll(lambda: snap(id)["operation"]["native_session_id"] is not None)
            request(dict(command="revoke", grant_ref="grant-batch_active-1"), True)
            require(
                submit_batch([c, e, d])["status"] == "admitted",
                "superseding active delete batch",
            )
            poll(lambda: snap(id)["operation"]["cancellation"] == "acknowledged")
            row["terminal"] = "cancelled"
            save()
            poll(lambda: snap(id)["operation"]["state"] == "observed_cancelled")
            require(
                snap(id)["state"]["superseded_without_execution"] == [2],
                "missing edit was executed",
            )
            require(
                receipt([c, e, d])["batch_sha256"]
                == hashlib.sha256(batch_wire([c, e, d])).hexdigest(),
                "receipt not exact",
            )
            checks.append(
                "atomic_latest_chain_supersession_missing_edits_and_exact_lost_ack_readback"
            )
            # Actual Tether journal survives a process exit after local commit,
            # before any Core submission. Later edits/delete arrive while offline.
            database = directory / "tether-semantic.sqlite"
            producer = [
                sys.executable,
                str(ROOT / "tools/intent_fixture_producer.py"),
                "--tether",
                tether,
                "--database",
                str(database),
            ]
            crashed = subprocess.run(
                [*producer, "--action", "crash-after-commit"],
                capture_output=True,
                timeout=30,
            )
            require(crashed.returncode == 91, "producer crash checkpoint")

            def produce(action: str) -> dict[str, Any]:
                return cast(
                    dict[str, Any],
                    json.loads(
                        subprocess.check_output(
                            [*producer, "--action", action], timeout=30
                        )
                    ),
                )

            first_batch = produce("snapshot")
            require(len(first_batch["intents"]) == 1, "lost atomic first candidate")
            produce("retain-2")
            journal_batch = produce("retain-3")
            journal_wire = canonical(journal_batch)
            require(
                produce("snapshot") == journal_batch, "fresh producer snapshot mismatch"
            )
            only_latest = canonical(journal_batch["intents"][-1])
            issue(only_latest)
            start(canonical(journal_batch["intents"][0]))
            before_effects = events()
            require(
                request(dict(command="admit_intent_batch", wire=journal_wire.decode()))[
                    "status"
                ]
                == "admitted",
                "journal batch admission",
            )
            require(
                events() == before_effects,
                "offline retained create/edit caused stale effects",
            )
            from tether.semantic_journal import (  # type: ignore[import-not-found]
                SemanticAdmission,
            )

            journal = SemanticAdmission(
                str(database), app_id="app-1", account_id="account-1"
            )
            try:
                task = journal_batch["intents"][-1]["scoped"]["request"]["binding"][
                    "task_id"
                ]
                require(
                    journal.pending_batch(task) == journal_wire,
                    "lost reply marked delivered",
                )
                # Deliberately discard submit acknowledgment, reconcile through Core query.
                journal.reconcile_retention(
                    journal_wire,
                    lambda b: request(dict(command="intent_receipt", wire=b.decode())),
                )
                require(
                    journal.pending_batch(task) is None,
                    "exact Core receipt not retained",
                )
            finally:
                journal.close()
            checks.append(
                "actual_tether_atomic_journal_crash_offline_edits_delete_and_receipt_recovery"
            )
            ledger = events()
            before = {id: snap(id) for id in ids}
            if prior:
                for identifier in ids:
                    history = request(dict(command="history", workflow_id=identifier))
                    prior_histories[identifier] = hashlib.sha256(
                        json.dumps(history, sort_keys=True).encode()
                    ).hexdigest()
                require(events() == ledger, "old history inspection changed effects")
            lane.restart(abrupt=True)
            worker2 = lane.spawn_client(
                image,
                [
                    "intent-worker",
                    "http://temporal:7233",
                    "/effects",
                    "/effects/authority-helper.json",
                ],
                mounts=mounts,
            )
            require(
                {id: snap(id) for id in ids} == before,
                "fresh SQLite restart changed state",
            )
            require(admit(deleted) == "duplicate", "restart tombstone replay")
            histories = {}
            for id in ids:
                h = request(dict(command="history", workflow_id=id))
                path = directory / f"history-{id}.json"
                path.write_text(json.dumps(h))
                histories[id] = hashlib.sha256(path.read_bytes()).hexdigest()
                require(
                    json.loads(
                        lane.run_client(
                            image,
                            ["replay", "/effects/" + path.name],
                            mounts=mounts,
                            deadline=60,
                        ).stdout
                    )["status"]
                    == "passed",
                    "intent replay",
                )
            require(events() == ledger, "replay caused effects")
            require(
                sum(e["action"] == "launch" for e in ledger) == 6,
                "unexpected launch count",
            )
            require(
                sum(e["action"] == "cancel" for e in ledger) == 3,
                "unexpected cancel count",
            )
            checks.append(
                "ordered_revisions_fresh_process_store_recovery_and_history_replay"
            )
        for kind, names in [
            ("container", [lane.server, worker, worker2]),
            ("volume", [lane.volume]),
            ("network", [lane.network]),
        ]:
            for name in names:
                require(
                    subprocess.run(
                        ["docker", kind, "inspect", name],
                        capture_output=True,
                        timeout=30,
                    ).returncode
                    != 0,
                    "leaked " + kind,
                )
    return dict(
        status="passed",
        upgrade_from=prior or "not_run",
        old_executable_history_digests=prior_histories,
        evidence_class="isolated_integration",
        checks=checks,
        image_id=image,
        binary_sha256=manifest["binary_sha256"],
        source_hashes=manifest["source_hashes"],
        history_digests=histories,
        launch_count=6,
        cancel_count=3,
        cleanup="passed",
        live_acceptance="not_run",
        boundaries=dict(
            core="real",
            temporal="real_SQLite",
            tether="real_producer_codec",
            issuer="synthetic_ephemeral",
            host="scripted_peer",
            slack_transport="not_exercised",
        ),
    )


if __name__ == "__main__":

    def cancelled(_signal: int, _frame: Any) -> None:
        raise KeyboardInterrupt("bounded intent qualifier cancelled")

    signal.signal(signal.SIGTERM, cancelled)
    p = argparse.ArgumentParser()
    p.add_argument("--platform", required=True)
    p.add_argument("--platform-prepared", required=True)
    p.add_argument("--prepared", required=True)
    p.add_argument("--tether", required=True)
    p.add_argument("--report", required=True)
    p.add_argument("--probe", choices=["failure", "cancel"])
    p.add_argument("--probe-report")
    p.add_argument("--prior-core")
    p.add_argument("--prior-prepared")
    a = p.parse_args()
    result = test(
        a.platform,
        a.platform_prepared,
        a.prepared,
        a.tether,
        a.probe,
        a.probe_report,
        a.prior_core,
        a.prior_prepared,
    )
    pathlib.Path(a.report).write_text(json.dumps(result, indent=2) + "\n")
    print(json.dumps(dict(status=result["status"], checks=result["checks"])))
