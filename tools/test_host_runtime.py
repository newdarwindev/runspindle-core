#!/usr/bin/env python3
"""H02: real core + pinned host library + Temporal, independently observed peer."""

from __future__ import annotations

import argparse
import concurrent.futures
import hashlib
import json
import pathlib
import sqlite3
import subprocess
import sys
import tempfile
import time
import types
import uuid
from contextlib import closing
from typing import Any, Callable

sys.path.insert(0, str(pathlib.Path(__file__).resolve().parents[1]))
from tools.runtime_artifact import validate

ROOT = pathlib.Path(__file__).resolve().parents[1]


def require(value: Any, message: str) -> None:
    if not value:
        raise AssertionError(message)


def wait(
    read: Callable[[], Any], predicate: Callable[[Any], Any], deadline: float = 35
) -> Any:
    end = time.monotonic() + deadline
    last = None
    while time.monotonic() < end:
        try:
            last = read()
            if predicate(last):
                return last
        except (subprocess.SubprocessError, RuntimeError, ValueError, sqlite3.Error):
            pass
        time.sleep(0.15)
    raise AssertionError("observation deadline: " + str(last))


def admission(seed: int) -> dict[str, Any]:
    return {
        "version": "runspindle.runtime.v1",
        "delivery_namespace": "synthetic",
        "delivery_id": f"delivery-{seed}",
        "task_id": f"task-{seed}",
        "revision": 1,
        "predecessor_revision": None,
        "operation_id": f"operation-{seed}",
        "normalized_digest": hashlib.sha256(f"synthetic-{seed}".encode()).hexdigest(),
        "policy_revision": "policy",
        "host_id": "explicit-host",
        "workspace_id": "workspace",
        "lease_epoch": 1,
        "grant_ref": "synthetic-grant",
    }


def run(
    platform: str,
    platform_prepared: str,
    core_prepared: str,
    seed: int = 17,
    crash_before: bool = False,
    observer: Callable[..., Any] | None = None,
) -> dict[str, Any]:
    manifest = validate(ROOT, core_prepared)
    require(
        manifest.get("features")
        in (
            "host-runtime",
            "host-runtime,artifact-store",
            "delivery-runtime",
            "verified-authority",
        ),
        "host-runtime feature not prepared",
    )
    source = pathlib.Path(platform) / "tools/temporal_lane.py"
    module = types.ModuleType("verified_lane")
    module.__file__ = str(source)
    exec(compile(source.read_bytes(), str(source), "exec"), module.__dict__)
    a = admission(seed)
    binding = {
        k: a[k]
        for k in [
            "task_id",
            "revision",
            "operation_id",
            "policy_revision",
            "host_id",
            "workspace_id",
            "lease_epoch",
        ]
    }
    request = {"version": "runspindle.integration.v1", "admission": a}
    script = {
        "request": request,
        "authorized": True,
        "online": False,
        "native_session_id": "observed-peer-session",
        "crash_after_launch": not crash_before,
        "crash_before_effect": crash_before,
        "lost_reply": True,
        "terminal": None,
    }
    with tempfile.TemporaryDirectory(prefix="core-host-lane-") as tmp:
        directory = pathlib.Path(tmp)
        mounts = [{"source": str(directory), "target": "/effects", "read_only": False}]

        def write() -> None:
            p = directory / "native-script.next"
            p.write_text(json.dumps(script))
            p.replace(directory / "native-script.json")

        def ledger() -> list[str]:
            p = directory / "native-peer.sqlite"
            if not p.exists():
                return []
            with closing(
                sqlite3.connect("file:" + str(p) + "?mode=rw", uri=True)
            ) as db:
                db.execute("PRAGMA query_only=ON")
                return [
                    x[0] for x in db.execute("SELECT action FROM calls ORDER BY id")
                ]

        write()
        with module.Lane(platform_prepared) as lane:

            def client(command: dict[str, Any]) -> Any:
                name = "command-" + uuid.uuid4().hex + ".json"
                (directory / name).write_text(json.dumps(command))
                raw = lane.run_client(
                    manifest["image_id"],
                    ["client", "http://temporal:7233", "/effects/" + name],
                    mounts=mounts,
                    deadline=45,
                )
                return json.loads(raw.stdout)

            def worker() -> Any:
                return lane.spawn_client(
                    manifest["image_id"],
                    ["worker", "http://temporal:7233", "/effects"],
                    mounts=mounts,
                )

            w = worker()
            registry = f"host-registry-{seed}"
            client({"command": "start_host", "registry_id": registry})

            def admit() -> Any:
                return client(
                    {
                        "command": "admit_host",
                        "registry_id": registry,
                        "request": request,
                    }
                )

            with concurrent.futures.ThreadPoolExecutor(max_workers=2) as pool:
                replies = list(pool.map(lambda _: admit(), range(2)))
            require(
                sorted(r["status"] for r in replies)
                == ["admitted", "duplicate_delivery"],
                "durable admission duplicate mismatch",
            )
            job = replies[0]["job_id"]

            def query() -> Any:
                return client({"command": "native", "job_id": job})

            wait(query, lambda s: s and s["execution"] == "Queued")
            require("launch" not in ledger(), "offline host launched")
            bad = json.loads(json.dumps(request))
            bad["admission"]["host_id"] = "other-host"
            require(
                client(
                    {"command": "admit_host", "registry_id": registry, "request": bad}
                )["status"]
                == "denied",
                "changed authority accepted",
            )
            script["online"] = True
            write()
            exit_code = wait(
                lambda: json.loads(
                    subprocess.check_output(["docker", "inspect", w], text=True)
                )[0]["State"],
                lambda s: not s["Running"],
            )["ExitCode"]
            require(
                exit_code == (91 if crash_before else 93),
                "worker interruption boundary missing",
            )
            require(ledger().count("launch") == 1, "dispatch count before restart")
            script["crash_after_launch"] = False
            script["crash_before_effect"] = False
            write()
            lane.restart(abrupt=True)
            w2 = worker()
            require(
                admit()["status"] == "duplicate_delivery",
                "admission lost across restart",
            )
            if crash_before:
                wait(query, lambda s: s and s["execution"] == "Unknown")
                wait(ledger, lambda calls: "discover" in calls)
                require(
                    ledger().count("launch") == 1,
                    "uncertain intent relaunched after interruption",
                )
                with closing(
                    sqlite3.connect(
                        "file:" + str(directory / "native-peer.sqlite") + "?mode=rw",
                        uri=True,
                    )
                ) as db:
                    require(
                        db.execute("SELECT count(*) FROM sessions").fetchone()[0] == 0,
                        "unexpected effect before peer interruption",
                    )
                result = {
                    "boundary": "before_peer_effect",
                    "launch_calls": 1,
                    "native_receipts": 0,
                    "unknown": True,
                }
            else:
                recovered = wait(
                    query,
                    lambda s: s and s["native_session_id"] == "observed-peer-session",
                )
                require(
                    recovered["binding"] == binding and recovered["last_sequence"] >= 2,
                    "host mapping/sequence mismatch",
                )
                wrong = dict(binding, lease_epoch=2)
                require(
                    not client(
                        {"command": "cancel_native", "job_id": job, "binding": wrong}
                    )["accepted"],
                    "stale cancellation accepted",
                )
                require(
                    client(
                        {"command": "cancel_native", "job_id": job, "binding": binding}
                    )["accepted"],
                    "cancel denied",
                )
                wait(query, lambda s: s and s["cancellation"] == "Acknowledged")
                script["terminal"] = "completed"
                write()
                terminal = wait(
                    query, lambda s: s and s["execution"] == {"Terminal": "Completed"}
                )
                require(
                    terminal["cancellation"] == "ObservedTerminal",
                    "cancel/completion race fabricated outcome",
                )
                calls = ledger()
                require(
                    calls.count("launch") == 1
                    and calls.count("cancel") == 1
                    and calls.index("launch")
                    < calls.index("discover")
                    < calls.index("cancel"),
                    "peer call count/order mismatch",
                )
                with closing(
                    sqlite3.connect(
                        "file:" + str(directory / "native-peer.sqlite") + "?mode=rw",
                        uri=True,
                    )
                ) as db:
                    observations = [
                        json.loads(x[0])
                        for x in db.execute("SELECT observation FROM sessions")
                    ]
                require(
                    len(observations) == 1
                    and observations[0]["binding"] == binding
                    and observations[0]["native_session_id"] == "observed-peer-session",
                    "independent native receipt mismatch",
                )
                history = client({"command": "history", "workflow_id": job})
                (directory / "history.json").write_text(json.dumps(history))
                before = len(ledger())
                replay = json.loads(
                    lane.run_client(
                        manifest["image_id"],
                        ["replay", "/effects/history.json"],
                        mounts=mounts,
                        deadline=45,
                    ).stdout
                )
                require(
                    replay["status"] == "passed" and len(ledger()) == before,
                    "replay created effect",
                )
                next_admission = admission(seed + 1)
                next_request = {
                    "version": "runspindle.integration.v1",
                    "admission": next_admission,
                }
                script["request"] = next_request
                script["online"] = False
                script["terminal"] = None
                write()
                next_reply = client(
                    {
                        "command": "admit_host",
                        "registry_id": registry,
                        "request": next_request,
                    }
                )
                require(
                    next_reply["status"] == "admitted",
                    "second bounded request rejected",
                )
                next_job = next_reply["job_id"]

                def next_query() -> Any:
                    return client({"command": "native", "job_id": next_job})

                wait(next_query, lambda s: s and s["execution"] == "Queued")
                next_binding = {k: next_admission[k] for k in binding}
                require(
                    client(
                        {
                            "command": "cancel_native",
                            "job_id": next_job,
                            "binding": next_binding,
                        }
                    )["accepted"],
                    "pre-dispatch cancel denied",
                )
                local = wait(
                    next_query,
                    lambda s: s and s["execution"] == "CancelledBeforeDispatch",
                )
                require(
                    local["native_session_id"] is None
                    and local["cancellation"] == "Requested",
                    "fabricated pre-dispatch native terminal",
                )
                subprocess.run(["docker", "kill", w2], check=True, capture_output=True)
                lane.restart(abrupt=True)
                worker()
                local = next_query()
                require(
                    local["execution"] == "CancelledBeforeDispatch"
                    and ledger().count("launch") == 1,
                    "pre-dispatch cancel lost after fresh process",
                )
                race_admission = admission(seed + 2)
                race_request = {
                    "version": "runspindle.integration.v1",
                    "admission": race_admission,
                }
                script["request"] = race_request
                script["online"] = True
                write()
                race_reply = client(
                    {
                        "command": "admit_host",
                        "registry_id": registry,
                        "request": race_request,
                    }
                )
                require(race_reply["status"] == "admitted", "race admission failed")
                race_job = race_reply["job_id"]
                race_binding = {k: race_admission[k] for k in binding}

                def race_query() -> Any:
                    return client({"command": "native", "job_id": race_job})

                wait(
                    race_query,
                    lambda s: s and s["native_session_id"] == "observed-peer-session",
                )
                script["reconcile_barrier"] = True
                script["terminal"] = "completed"
                write()
                ready = directory / (
                    "reconcile-ready-" + race_binding["operation_id"] + ".json"
                )
                held = wait(
                    lambda: json.loads(ready.read_text()) if ready.exists() else None,
                    lambda s: s is not None,
                )
                require(
                    held["execution"] == {"Terminal": "Completed"}
                    and held["cancellation"] == "None",
                    "terminal reply barrier missing actual host outcome",
                )
                require(
                    client(
                        {
                            "command": "cancel_native",
                            "job_id": race_job,
                            "binding": race_binding,
                        }
                    )["accepted"],
                    "cancel while terminal reply pending was denied",
                )
                pending = race_query()
                require(
                    pending["cancellation"] == "Requested",
                    "accepted pending cancel intent missing",
                )
                release = directory / (
                    "reconcile-release-" + race_binding["operation_id"] + ".json"
                )
                pending_release = release.with_suffix(".next")
                pending_release.write_text(json.dumps(race_binding))
                pending_release.replace(release)
                raced = wait(
                    race_query,
                    lambda s: s and s["execution"] == {"Terminal": "Completed"},
                )
                require(
                    raced["cancellation"] == "ObservedTerminal"
                    and raced["native_session_id"] == held["native_session_id"],
                    "completion race dropped accepted cancel or changed native outcome",
                )
                returned = directory / (
                    "reconcile-returned-" + race_binding["operation_id"] + ".json"
                )
                require(
                    returned.exists(),
                    "held native reply was not independently returned",
                )
                history = client({"command": "history", "workflow_id": race_job})
                accepted_ids = [
                    int(e["event_id"])
                    for e in history["events"]
                    if "WorkflowExecutionUpdateAcceptedEventAttributes"
                    in e.get("attributes", {})
                ]
                completed_ids = [
                    int(e["event_id"])
                    for e in history["events"]
                    if "ActivityTaskCompletedEventAttributes" in e.get("attributes", {})
                ]
                require(
                    accepted_ids and max(completed_ids) > max(accepted_ids),
                    "Temporal history did not place terminal activity return after cancel acceptance",
                )
                with closing(
                    sqlite3.connect(
                        "file:" + str(directory / "native-peer.sqlite") + "?mode=rw",
                        uri=True,
                    )
                ) as db:
                    race_calls = [
                        action
                        for action, b in db.execute(
                            "SELECT action,binding FROM calls ORDER BY id"
                        )
                        if json.loads(b) == race_binding
                    ]
                require(
                    race_calls.count("launch") == 1 and race_calls.count("cancel") == 0,
                    "completion race created duplicate launch or late cancellation effect",
                )
                result = {
                    "boundary": "after_peer_effect",
                    "launch_calls": 1,
                    "cancels": 1,
                    "session": "observed-peer-session",
                    "terminal": "completed",
                    "pre_dispatch_cancel": "durable_local_outcome",
                    "cancel_during_terminal_observation": "accepted_intent_preserved",
                    "replay": "passed",
                }
            if observer is not None:
                context = {
                    "request": request,
                    "before": crash_before,
                    "seed": seed,
                    "result": result,
                }
                if not crash_before:
                    context["terminal_race"] = {
                        "request": race_request,
                        "held": held,
                        "core": raced,
                        "accepted_events": accepted_ids,
                        "completed_events": completed_ids,
                    }
                observer(directory, context)
    return result


if __name__ == "__main__":
    p = argparse.ArgumentParser()
    p.add_argument("--platform", required=True)
    p.add_argument("--platform-prepared", required=True)
    p.add_argument("--core-prepared", default=str(ROOT / "local/runtime-prepared"))
    p.add_argument("--repeat", type=int, default=2)
    p.add_argument("--seed", type=int, default=17)
    a = p.parse_args()
    require(a.repeat >= 2, "at least two seeded repeats required")
    results = [
        [
            run(a.platform, a.platform_prepared, a.core_prepared, a.seed, before)
            for before in (False, True)
        ]
        for _ in range(a.repeat)
    ]
    require(all(r == results[0] for r in results), "normalized seeded outcomes differ")
    print(
        json.dumps(
            {
                "lane": "H02",
                "status": "passed",
                "evidence_class": "isolated_integration",
                "repeat_runs": a.repeat,
                "seed": a.seed,
                "normalized_outcomes": results[0],
                "scripted": ["native_peer"],
                "live_acceptance": "not_run",
            }
        )
    )
