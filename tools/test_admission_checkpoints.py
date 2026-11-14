#!/usr/bin/env python3
"""Real registry recovery at synthetic SDK command and client reply boundaries.

No file/network test checkpoint executes inside deterministic workflow logic.
Admission mutation and child intent can be one workflow-task transaction; the
pre-commit cases explicitly do not claim a separately durable mutation gap.
"""
import argparse
from concurrent.futures import ThreadPoolExecutor
from contextlib import closing
import hashlib
import json
from pathlib import Path
import sqlite3
import subprocess
import tempfile
import types
import uuid

from runtime_artifact import validate
from test_host_runtime import admission, require, wait

ROOT = Path(__file__).resolve().parents[1]
POINTS = (
    "admission_commit",
    "before_child_creation",
    "after_child_creation",
    "before_reply",
)


def run(platform, prepared, manifest, point, *, prior_image=None):
    path = Path(platform) / "tools/temporal_lane.py"
    module = types.ModuleType("checkpoint_lane")
    module.__file__ = str(path)
    exec(compile(path.read_bytes(), str(path), "exec"), module.__dict__)
    with tempfile.TemporaryDirectory(prefix="admission-checkpoint-") as temporary:
        directory = Path(temporary)
        mounts = [{"source": str(directory), "target": "/effects", "read_only": False}]
        request = {"version": "runspindle.integration.v1", "admission": admission(17)}
        script = {
            "request": request,
            "authorized": True,
            "online": False,
            "native_session_id": "synthetic-checkpoint-session",
            "crash_after_launch": False,
            "lost_reply": False,
            "terminal": None,
            "authority_valid_until_ms": None,
        }

        def write():
            next_file = directory / "script.next"
            next_file.write_text(json.dumps(script))
            next_file.replace(directory / "native-script.json")

        def effects():
            path = directory / "native-peer.sqlite"
            if not path.exists():
                return []
            with closing(
                sqlite3.connect("file:" + str(path) + "?mode=rw", uri=True)
            ) as db:
                db.execute("PRAGMA query_only=ON")
                return list(db.execute("SELECT action,binding FROM calls ORDER BY id"))

        write()
        with module.Lane(prepared) as lane:

            def command(value, checkpoint=None):
                name = uuid.uuid4().hex + ".json"
                (directory / name).write_text(json.dumps(value))
                args = ["client", "http://temporal:7233", "/effects/" + name]
                if checkpoint:
                    args += [checkpoint]
                return lane.run_client(
                    manifest["image_id"], args, mounts=mounts, deadline=60
                )

            def client(value):
                return json.loads(command(value).stdout)

            def worker(checkpoint=None, image=None):
                args = ["worker", "http://temporal:7233", "/effects"]
                if checkpoint:
                    args += [checkpoint]
                return lane.spawn_client(
                    image or manifest["image_id"], args, mounts=mounts
                )

            registry = "checkpoint-registry"
            w = worker(None if point == "before_reply" else point, image=prior_image)
            client({"command": "start_host", "registry_id": registry})
            value = {
                "command": "admit_host",
                "registry_id": registry,
                "request": request,
            }
            job = (
                "job-"
                + hashlib.sha256(
                    (request["admission"]["task_id"] + "\n1").encode()
                ).hexdigest()
            )
            with ThreadPoolExecutor(max_workers=1) as pool:
                future = pool.submit(
                    command, value, "before_reply" if point == "before_reply" else None
                )
                if point == "before_reply":
                    # run_client rejects a nonzero exit, but the container is owned
                    # and the lost reply is reconciled independently below.
                    try:
                        completed = future.result(timeout=65)
                        require(
                            completed.returncode == 96,
                            "client did not stop before reply",
                        )
                    except RuntimeError as error:
                        require(
                            "client failed with exit 96:" in str(error),
                            "unexpected client failure",
                        )
                    subprocess.run(
                        ["docker", "kill", w],
                        check=True,
                        capture_output=True,
                        timeout=20,
                    )
                else:
                    state = wait(
                        lambda: json.loads(
                            subprocess.check_output(["docker", "inspect", w], text=True)
                        )[0]["State"],
                        lambda s: not s["Running"],
                    )
                    require(state["ExitCode"] == 95, "SDK boundary not interrupted")
                    require(
                        json.loads(
                            (directory / "admission-checkpoint.json").read_bytes()
                        )["point"]
                        == point,
                        "checkpoint identity changed",
                    )
                before = client({"command": "history", "workflow_id": registry})
                require(not effects(), "offline pinned peer dispatched")
                script["authority_valid_until_ms"] = 0
                write()
                lane.restart(abrupt=True)
                w = worker()
                require(
                    client(value)["status"] == "denied",
                    "expired replay renewed authority",
                )
                require(not effects(), "expired recovered authority caused host effect")
                script["authority_valid_until_ms"] = None
                write()
                reply = client(value)
                require(
                    reply["status"] in ("admitted", "duplicate_delivery")
                    and reply["job_id"] == job,
                    "recovery changed stable job",
                )
                if point != "before_reply":
                    response = json.loads(future.result(timeout=65).stdout)
                    require(response["job_id"] == job, "original update changed job")

            def query():
                return client({"command": "native", "job_id": job})

            wait(query, lambda s: s and s["execution"] == "Queued")
            registry_state = client(
                {"command": "host_registry", "registry_id": registry}
            )
            require(
                len(registry_state["revisions"]) == 1
                and len({a["task_id"] for a in registry_state["revisions"].values()})
                == 1,
                "admission duplicated task/revision",
            )
            script["online"] = True
            script["terminal"] = "completed"
            write()
            wait(query, lambda s: s and s["execution"] == {"Terminal": "Completed"})
            wait(
                lambda: client({"command": "host_registry", "registry_id": registry}),
                lambda s: job in s["terminal"],
            )
            observed = effects()
            launches = [
                json.loads(binding)
                for action, binding in observed
                if action == "launch"
            ]
            require(
                len(launches) == 1 and launches[0]["host_id"] == "explicit-host",
                "duplicate or alternate host effect",
            )
            counts = {}
            for identifier in (registry, job):
                history = client({"command": "history", "workflow_id": identifier})
                counts[identifier] = sum(
                    "ChildWorkflowExecutionStartedEventAttributes"
                    in e.get("attributes", {})
                    for e in history["events"]
                )
                (directory / "history.json").write_text(json.dumps(history))
                replay = lane.run_client(
                    manifest["image_id"],
                    ["replay", "/effects/history.json"],
                    mounts=mounts,
                    deadline=45,
                )
                require(
                    json.loads(replay.stdout)["status"] == "passed",
                    "history compatibility failed",
                )
            require(
                counts[registry] == 1 and effects() == observed,
                "duplicate child or replayed effect",
            )
            before_children = sum(
                "ChildWorkflowExecutionStartedEventAttributes"
                in e.get("attributes", {})
                for e in before["events"]
            )
            require(
                before_children
                == (0 if point in ("admission_commit", "before_child_creation") else 1),
                "unexpected durable boundary",
            )
            return {
                "point": point,
                "before_restart_children": before_children,
                "recovered_tasks": 1,
                "recovered_children": 1,
                "host_launches": 1,
                "expired_authority": "denied_no_effect",
                "history_replay": "passed_no_effect",
                "boundary": (
                    "pre_server_commit_commands"
                    if before_children == 0
                    else "durable_child_before_reply_or_activation"
                ),
                "prior_worker": bool(prior_image),
            }


def prior_image(root, prepared):
    """Repackage retained qualified bytes if Docker discarded the old manifest.

    This is a new compatibility-only image identity, explicitly bound to the old
    source/binary record. It does not relabel that image as the old artifact.
    """
    from prepare_temporal_runtime import sha
    import shutil

    root, prepared = Path(root), Path(prepared)
    declared = json.loads((root / "docs/runtime-build.json").read_bytes())
    retained = json.loads((prepared / "manifest.json").read_bytes())
    require(declared == retained, "prior prepared record mismatch")
    paths = [
        root / "Cargo.toml",
        root / "Cargo.lock",
        root / "temporal/toolchain.json",
        *sorted((root / "src").rglob("*.rs")),
    ]
    require(
        {str(p.relative_to(root)): sha(p) for p in paths} == declared["source_hashes"],
        "prior source bytes changed",
    )
    require(
        sha(prepared / "temporal-runtime") == declared["binary_sha256"],
        "prior executable bytes changed",
    )
    require(
        sha(root / "docs/runtime-dependencies.json")
        == declared["dependency_inventory_sha256"],
        "prior inventory changed",
    )
    with tempfile.TemporaryDirectory(prefix="prior-compatibility-image-") as temporary:
        directory = Path(temporary)
        shutil.copyfile(prepared / "temporal-runtime", directory / "temporal-runtime")
        (directory / "temporal-runtime").chmod(0o755)
        shutil.copytree(prepared / "notices", directory / "notices")
        shutil.copyfile(
            prepared / "runtime-dependencies.json",
            directory / "runtime-dependencies.json",
        )
        # The retained original recipe has an immutable base and no RUN/download.
        recipe = (prepared / "Dockerfile").read_text()
        base = json.loads((root / "temporal/toolchain.json").read_bytes())["rust_image"]
        require(
            recipe.startswith("FROM " + base + "\n") and "\nRUN " not in recipe,
            "unsafe prior repackaging recipe",
        )
        (directory / "Dockerfile").write_text(recipe)
        subprocess.run(
            [
                "docker",
                "build",
                "--network",
                "none",
                "--pull=false",
                "-t",
                "runspindle-core-prior:compatibility",
                str(directory),
            ],
            check=True,
            capture_output=True,
            timeout=120,
        )
    image = subprocess.check_output(
        [
            "docker",
            "image",
            "inspect",
            "runspindle-core-prior:compatibility",
            "--format",
            "{{.Id}}",
        ],
        text=True,
    ).strip()
    return image, {
        "source_sha256": declared["source_sha256"],
        "binary_sha256": declared["binary_sha256"],
        "compatibility_image_id": image,
        "original_image_id": declared["image_id"],
        "scope": "retained_original_executable_repackaged_no_rebuild",
    }


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--platform", required=True)
    parser.add_argument("--platform-prepared", required=True)
    parser.add_argument("--core-prepared", required=True)
    parser.add_argument("--prior-core")
    parser.add_argument("--prior-prepared")
    parser.add_argument("--report", required=True)
    args = parser.parse_args()
    manifest = validate(ROOT, args.core_prepared)
    outcomes = []
    for repeat in range(2):
        result = []
        for point in POINTS:
            outcome = run(args.platform, args.platform_prepared, manifest, point)
            result.append(outcome)
            print(json.dumps({"repeat": repeat, "outcome": outcome}), flush=True)
        outcomes.append(result)
    require(outcomes[0] == outcomes[1], "normalized checkpoint outcomes changed")
    prior = None
    if args.prior_core and args.prior_prepared:
        image, binding = prior_image(args.prior_core, args.prior_prepared)
        prior = {
            "outcome": run(
                args.platform,
                args.platform_prepared,
                manifest,
                "before_reply",
                prior_image=image,
            ),
            "binding": binding,
        }
    report = {
        "status": "passed",
        "evidence_class": "isolated_integration",
        "repeat_runs": 2,
        "normalized_outcomes": outcomes[0],
        "prior_history_compatibility": prior or "not_run",
        "native_wire": "blocked",
        "live_acceptance": "not_run",
    }
    Path(args.report).write_text(json.dumps(report, indent=2) + "\n")
    print(json.dumps(report))


if __name__ == "__main__":
    main()
