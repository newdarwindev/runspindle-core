#!/usr/bin/env python3
"""Authenticated host admission through real Temporal; scripted trust/native peer.

No native wire or Tether claim. An independent SQLite peer ledger counts effects.
"""
import argparse
from concurrent.futures import ThreadPoolExecutor
import copy
import hashlib
import json
from pathlib import Path
import sqlite3
import subprocess
import tempfile
import types
import uuid
from contextlib import closing

from runtime_artifact import validate
from test_host_runtime import admission, require, wait

ROOT = Path(__file__).resolve().parents[1]


def run(platform, platform_prepared, core_prepared, seed=17):
    manifest = validate(ROOT, core_prepared)
    require(manifest.get("features") == "delivery-runtime", "complete runtime required")
    path = Path(platform) / "tools/temporal_lane.py"
    module = types.ModuleType("admission_lane")
    module.__file__ = str(path)
    exec(compile(path.read_bytes(), str(path), "exec"), module.__dict__)
    with tempfile.TemporaryDirectory(prefix="admission-recovery-") as temporary:
        directory = Path(temporary)
        mounts = [{"source": str(directory), "target": "/effects", "read_only": False}]
        request = {"version": "runspindle.integration.v1", "admission": admission(seed)}
        script = {"request": request, "authorized": True, "online": False,
                  "native_session_id": "independently-observed-session", "crash_after_launch": False,
                  "lost_reply": False, "terminal": None, "crash_after_prepare": True}

        def write():
            pending = directory / "native-script.next"
            pending.write_text(json.dumps(script))
            pending.replace(directory / "native-script.json")

        def calls():
            path = directory / "native-peer.sqlite"
            if not path.exists():
                return []
            with closing(sqlite3.connect("file:" + str(path) + "?mode=rw", uri=True)) as db:
                db.execute("PRAGMA query_only=ON")
                return [(action, json.loads(b)) for action, b in db.execute("SELECT action,binding FROM calls ORDER BY id")]

        write()
        with module.Lane(platform_prepared) as lane:
            def client(command):
                name = "command-" + uuid.uuid4().hex + ".json"
                (directory / name).write_text(json.dumps(command))
                result = lane.run_client(manifest["image_id"], ["client", "http://temporal:7233", "/effects/" + name], mounts=mounts, deadline=45)
                return json.loads(result.stdout)

            def worker():
                return lane.spawn_client(manifest["image_id"], ["worker", "http://temporal:7233", "/effects"], mounts=mounts)

            registry = "admission-registry"
            w = worker()
            client({"command": "start_host", "registry_id": registry})

            def admit(r=request):
                return client({"command": "admit_host", "registry_id": registry, "request": r})

            script["authorized"] = False
            write()
            require(admit()["status"] == "denied", "actor/grant reference granted authority")
            script["authorized"] = True
            script["authority_valid_until_ms"] = 0
            write()
            require(admit()["status"] == "denied", "expired authority admitted")
            require(not calls(), "denied admission called native peer")
            script["authority_valid_until_ms"] = None
            write()
            with ThreadPoolExecutor(max_workers=4) as pool:
                replies = list(pool.map(lambda _: admit(), range(4)))
            require(sorted(r["status"] for r in replies) == ["admitted"] + ["duplicate_delivery"] * 3, "concurrent admission duplicated")
            job = "job-" + hashlib.sha256((request["admission"]["task_id"] + "\n1").encode()).hexdigest()
            require({r["job_id"] for r in replies} == {job}, "task identity changed")
            exit_code = wait(lambda: json.loads(subprocess.check_output(["docker", "inspect", w], text=True))[0]["State"], lambda s: not s["Running"])["ExitCode"]
            require(exit_code == 94, "did not interrupt after host prepare commit before child activity reply")
            require(not calls(), "prepare launched native peer")
            # Drop the admission reply as a caller could; reconstruct server and worker.
            script["crash_after_prepare"] = False
            script["authority_valid_until_ms"] = 0
            write()
            lane.restart(abrupt=True)
            w = worker()
            require(admit()["status"] == "denied", "recovery restored expired admission authority")
            require(not calls(), "expired recovered authority dispatched")
            script["authority_valid_until_ms"] = None
            write()
            require(admit() == {"status": "duplicate_delivery", "job_id": job}, "lost admission reply created another job")
            query = lambda: client({"command": "native", "job_id": job})
            wait(query, lambda s: s and s["execution"] == "Queued")
            # Distinguish repeated intent, conflicting delivery and explicit revision.
            another = copy.deepcopy(request)
            another["admission"]["delivery_id"] = "same-intent-new-delivery"
            script["request"] = another
            write()
            same = admit(another)
            require(same["job_id"] == job and same["status"] == "existing_revision", "new delivery duplicated semantic intent")
            conflict = copy.deepcopy(request)
            conflict["admission"]["host_id"] = "alternate-host"
            script["request"] = conflict
            write()
            require(admit(conflict)["status"] == "delivery_conflict", "authenticated conflicting replay accepted")
            revision = copy.deepcopy(request)
            revision["admission"].update(delivery_id="explicit-revision", revision=2, predecessor_revision=1, operation_id="second-operation")
            script["request"] = revision
            write()
            require(admit(revision)["status"] == "predecessor_active", "active predecessor bypassed")
            script["request"] = request
            script["online"] = True
            script["terminal"] = "completed"
            write()
            wait(query, lambda s: s and s["execution"] == {"Terminal": "Completed"})
            wait(lambda: client({"command": "host_registry", "registry_id": registry}), lambda s: job in s["terminal"])
            script["request"] = revision
            write()
            next_reply = admit(revision)
            require(next_reply["status"] == "admitted", "explicit successor denied after observed predecessor")
            wait(lambda: client({"command": "native", "job_id": next_reply["job_id"]}), lambda s: s and s["execution"] == {"Terminal": "Completed"})
            observed = calls()
            launches = [b for action, b in observed if action == "launch"]
            require([b["revision"] for b in launches] == [1, 2], "predecessor launch ordering/duplicate violated")
            require(all(b["host_id"] == "explicit-host" for b in launches), "alternate host dispatch")
            histories = {}
            for workflow in (registry, job, next_reply["job_id"]):
                history = client({"command": "history", "workflow_id": workflow})
                histories[workflow] = history
                history_path = directory / "history.json"
                history_path.write_text(json.dumps(history))
                replay = lane.run_client(manifest["image_id"], ["replay", "/effects/history.json"], mounts=mounts, deadline=45)
                require(json.loads(replay.stdout)["status"] == "passed", "history replay failed")
            require(calls() == observed, "history replay invoked native effect")
            starts = [e for e in histories[registry]["events"] if "ChildWorkflowExecutionStartedEventAttributes" in e.get("attributes", {})]
            require(len(starts) == 2, "duplicate child workflow after admission recovery")
            return {"seed": seed, "admission_callers": 4, "child_workflows": 2, "launch_revisions": [b["revision"] for b in launches],
                    "authority": "scripted_exact_request_default_deny_expiry", "recovery": "fresh_server_worker_after_host_prepare_commit_before_activity_reply",
                    "replay": "registry_and_children_no_effects", "native_wire": "blocked"}


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--platform", required=True)
    parser.add_argument("--platform-prepared", required=True)
    parser.add_argument("--core-prepared", required=True)
    parser.add_argument("--report", required=True)
    args = parser.parse_args()
    results = [run(args.platform, args.platform_prepared, args.core_prepared) for _ in range(2)]
    require(results[0] == results[1], "normalized seeded outcomes changed")
    report = {"status": "passed", "evidence_class": "isolated_integration", "repeat_runs": 2, "normalized_outcome": results[0], "live_acceptance": "not_run"}
    Path(args.report).write_text(json.dumps(report, indent=2) + "\n")
    print(json.dumps(report))
