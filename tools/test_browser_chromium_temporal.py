#!/usr/bin/env python3
"""Native Temporal/Core/Chromium in a disposable unprivileged loopback sandbox."""

from __future__ import annotations

import argparse
import copy
import hashlib
import json
import os
import pwd
import signal
import sqlite3
import subprocess
import sys
import tempfile
import threading
import time
from pathlib import Path
from typing import Any, Callable

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
from tools.runtime_artifact import validate  # noqa: E402


def check(value: Any, message: str) -> None:
    if not value:
        raise RuntimeError(message)


def digest(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def poll(fn: Callable[[], Any], seconds: float = 30) -> Any:
    end = time.monotonic() + seconds
    while time.monotonic() < end:
        value = fn()
        if value:
            return value
        time.sleep(0.1)
    raise RuntimeError("bounded native poll expired")


def worker(mode: str) -> dict[str, Any]:
    check(os.geteuid() != 0, "worker must be unprivileged")
    interfaces = {
        line.split(":", 1)[0].strip()
        for line in Path("/proc/self/net/dev").read_text().splitlines()
        if ":" in line
    }
    check(interfaces == {"lo"}, "loopback-only namespace required")
    root = Path("/effects")
    peer = root / "browser-peer"
    peer.mkdir()
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
    payload = b"synthetic-chromium-temporal-effect"
    (root / "browser-payload.bin").write_bytes(payload)
    (peer / "payload.bin").write_bytes(payload)
    (peer / "target.json").write_text(json.dumps(target))
    (peer / "crash-core-after-exchange").touch()
    if mode == "before":
        (peer / "crash-before-site").touch()
    script = {
        "target": target,
        "expected_sha256": hashlib.sha256(payload).hexdigest(),
        "dispatch_authorized": True,
        "reconcile_authorized": True,
        "not_after_unix_ms": int(time.time() * 1000) + 300000,
        "crash_before_effect": False,
        "crash_after_effect": False,
        "lost_reply": True,
    }

    def install() -> None:
        pending = root / "browser-script.next"
        pending.write_text(json.dumps(script))
        pending.replace(root / "browser-script.json")

    install()
    owned: list[subprocess.Popen[bytes]] = []

    def spawn(args: list[str]) -> subprocess.Popen[bytes]:
        p = subprocess.Popen(args, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
        owned.append(p)
        return p

    def stop(p: subprocess.Popen[bytes]) -> None:
        if p.poll() is None:
            p.kill()
        p.wait(timeout=5)

    def server() -> subprocess.Popen[bytes]:
        p = spawn(
            [
                "/temporal",
                "server",
                "start-dev",
                "--ip",
                "127.0.0.1",
                "--headless",
                "--db-filename",
                "/effects/temporal.sqlite",
                "--log-level",
                "error",
            ]
        )

        def healthy() -> bool:
            check(p.poll() is None, "Temporal exited during startup")
            return (
                subprocess.run(
                    [
                        "/temporal",
                        "operator",
                        "cluster",
                        "health",
                        "--address",
                        "127.0.0.1:7233",
                        "--command-timeout",
                        "1s",
                    ],
                    stdout=subprocess.DEVNULL,
                    stderr=subprocess.DEVNULL,
                    timeout=3,
                ).returncode
                == 0
            )

        poll(healthy)
        return p

    sequence = 0

    def client(command: dict[str, Any]) -> Any:
        nonlocal sequence
        sequence += 1
        path = root / f"command-{sequence}.json"
        path.write_text(json.dumps(command))
        result = subprocess.run(
            ["/core-runtime", "client", "http://temporal:7233", str(path)],
            text=True,
            capture_output=True,
            timeout=20,
        )
        if result.returncode:
            raise RuntimeError(result.stderr[-2000:])
        return json.loads(result.stdout)

    def rows(table: str, site: bool = False) -> list[Any]:
        path = peer / ("site.sqlite" if site else "transport.sqlite")
        if not path.exists():
            return []
        with sqlite3.connect(f"file:{path}?mode=ro", uri=True) as db:
            return db.execute(f"SELECT * FROM {table}").fetchall()

    def events() -> list[Any]:
        return rows("events", True)

    def duplicate() -> None:
        try:
            client({"command": "start_browser_engine", "target": target})
        except RuntimeError as error:
            check("already" in str(error).lower(), "unrelated duplicate failure")
        else:
            raise RuntimeError("duplicate start accepted")

    try:
        srv = server()
        first = spawn(
            [
                "/core-runtime",
                "browser-worker-chromium-fixture",
                "http://temporal:7233",
                "/effects",
            ]
        )
        workflow = client({"command": "start_browser_engine", "target": target})[
            "workflow_id"
        ]
        duplicate()
        try:
            poll(lambda: first.poll() is not None)
        except RuntimeError as error:
            raise RuntimeError(
                "worker failed to reach deliberate crash; snapshot="
                + json.dumps(
                    client({"command": "browser_engine", "workflow_id": workflow})
                )
                + "; exchanges="
                + repr(rows("exchanges"))
                + "; calls="
                + repr(rows("calls"))
            ) from error
        check(first.returncode == 95, "unexpected initial worker exit")
        check(
            len(events()) == (1 if mode == "after" else 0),
            "independent initial effects",
        )
        check(rows("exchanges") == [("dispatch",)], "receiver initial exchange count")
        check(rows("calls") == [("submit",)], "initial site call count")
        (peer / "crash-core-after-exchange").unlink()
        script["reconcile_authorized"] = False
        install()
        stop(srv)
        srv = server()
        spawn(
            [
                "/core-runtime",
                "browser-worker-chromium-fixture",
                "http://temporal:7233",
                "/effects",
            ]
        )

        def snapshot() -> Any:
            return client({"command": "browser_engine", "workflow_id": workflow})

        recovered = poll(
            lambda: (s if (s := snapshot())["dispatch_finished"] else None)
        )
        check(
            recovered["historical"]["outcome"] == "unknown", "recovery invented success"
        )
        check(rows("calls") == [("submit",)], "retry reentered site")
        wrong = copy.deepcopy(target)
        wrong["binding"]["epoch"] = 2
        check(
            not client(
                {
                    "command": "reconcile_browser_engine",
                    "workflow_id": workflow,
                    "target": wrong,
                }
            )["accepted"],
            "changed target accepted",
        )

        def reconcile(count: int) -> Any:
            check(
                client(
                    {
                        "command": "reconcile_browser_engine",
                        "workflow_id": workflow,
                        "target": target,
                    }
                )["accepted"],
                "reconcile not queued",
            )
            return poll(
                lambda: (
                    s if (s := snapshot())["completed_reconciles"] == count else None
                )
            )

        denied = reconcile(1)
        check(denied["historical"] is None, "Core denial exposed success")
        check(rows("exchanges") == [("dispatch",)], "Core denial entered Browser")
        script["reconcile_authorized"] = True
        install()
        (peer / "revoked").touch()
        denied = reconcile(2)
        check(
            denied["historical"]["outcome"] == "unknown",
            "Browser denial exposed success",
        )
        check(rows("calls") == [("submit",)], "Browser denial entered site")
        (peer / "revoked").unlink()
        final = reconcile(3)
        check(
            final["historical"]["outcome"]
            == ("confirmed" if mode == "after" else "unknown"),
            "wrong final outcome",
        )
        check(
            rows("calls") == [("submit",), ("readback",)], "unexpected action ordering"
        )
        check(
            rows("exchanges") == [("dispatch",), ("reconcile",), ("reconcile",)],
            "unexpected receiver exchanges",
        )
        check(len(events()) == (1 if mode == "after" else 0), "recovery resubmitted")
        if mode == "after":
            stored, dig, outcome = events()[0]
            check(
                json.loads(stored) == target["binding"]
                and dig == hashlib.sha256(payload).hexdigest()
                and outcome == "confirmed",
                "wrong independent effect bytes/identity",
            )
            poll(
                lambda: any(
                    "WorkflowExecutionCompletedEventAttributes"
                    in e.get("attributes", {})
                    for e in client({"command": "history", "workflow_id": workflow})[
                        "events"
                    ]
                )
            )
            duplicate()
        history = client({"command": "history", "workflow_id": workflow})
        path = root / "history.json"
        path.write_text(json.dumps(history))
        before = (rows("calls"), rows("exchanges"), events())
        replay = subprocess.run(
            ["/core-runtime", "replay", str(path)],
            text=True,
            capture_output=True,
            timeout=30,
        )
        check(
            replay.returncode == 0 and json.loads(replay.stdout)["status"] == "passed",
            "actual history replay failed",
        )
        check(
            (rows("calls"), rows("exchanges"), events()) == before,
            "replay entered browser/site",
        )
        return {
            "mode": mode,
            "status": "passed",
            "final": final,
            "site_effects": len(events()),
            "site_calls": len(rows("calls")),
            "browser_exchanges": len(rows("exchanges")),
            "history_sha256": digest(path),
            "optimization_level": sys.flags.optimize,
        }
    finally:
        for process in reversed(owned):
            stop(process)


def bounded(command: list[str], timeout: float = 210) -> None:
    def terminate(signum: int, frame: Any) -> None:
        raise SystemExit(128 + signum)

    previous = signal.signal(signal.SIGTERM, terminate)
    try:
        _bounded(command, timeout)
    finally:
        signal.signal(signal.SIGTERM, previous)


def _bounded(command: list[str], timeout: float) -> None:
    """Bound memory/time and kill owned wrapper group before reaping its PID."""
    output = bytearray()
    overflow = threading.Event()
    process = subprocess.Popen(
        command,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        start_new_session=True,
    )
    if process.stdout is None:
        process.kill()
        process.wait(timeout=5)
        raise RuntimeError("missing worker output")
    stream = process.stdout

    def drain() -> None:
        while chunk := stream.read(8192):
            if len(output) + len(chunk) > 1048576:
                overflow.set()
                return
            output.extend(chunk)

    reader = threading.Thread(target=drain, daemon=True)
    try:
        reader.start()
        end = time.monotonic() + timeout
        while True:
            check(not overflow.is_set(), "native worker output overflow")
            check(time.monotonic() < end, "native worker deadline")
            exited = os.waitid(
                os.P_PID, process.pid, os.WEXITED | os.WNOHANG | os.WNOWAIT
            )
            if exited is not None:
                break
            time.sleep(0.05)
        reader.join(2)
        check(
            not reader.is_alive() and not overflow.is_set(), "native output incomplete"
        )
        check(
            exited.si_code == os.CLD_EXITED and exited.si_status == 0,
            "native lane failed: " + output.decode("utf-8", errors="replace")[-4000:],
        )
    finally:
        try:
            os.killpg(process.pid, signal.SIGKILL)
        except ProcessLookupError:
            pass
        process.wait(timeout=5)
        if reader.ident is not None:
            reader.join(2)
        stream.close()
        check(not reader.is_alive(), "native output reader leaked")


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--worker", choices=["before", "after"])
    parser.add_argument("--core-prepared", type=Path)
    parser.add_argument("--temporal-prepared", type=Path)
    parser.add_argument("--browser-source", type=Path)
    parser.add_argument("--user", default="standard")
    parser.add_argument("--report", type=Path, required=True)
    args = parser.parse_args()
    if args.worker:
        args.report.write_text(json.dumps(worker(args.worker), indent=2) + "\n")
        return
    check(
        os.geteuid() == 0,
        "outer verifier needs local Docker access; worker drops privileges",
    )
    check(
        args.core_prepared and args.temporal_prepared and args.browser_source,
        "explicit prepared inputs required",
    )
    manifest = validate(ROOT, args.core_prepared)
    binary = args.core_prepared / "temporal-runtime"
    check(
        digest(binary) == manifest["binary_sha256"],
        "native Core executable differs from qualified build",
    )
    temporal = args.temporal_prepared / "temporal"
    check(
        digest(temporal)
        == "6968c86c97d91d4b49e19c0c9b0842bfb5eb2d356f22b989006d1e09164eab5b",
        "Temporal executable pin mismatch",
    )
    user = pwd.getpwnam(args.user)
    check(user.pw_uid != 0, "worker user cannot be root")
    browser_pin = json.loads(
        (ROOT / "tests/fixtures/browser_chromium_source_pin.json").read_text()
    )["files"]

    def input_hashes() -> dict[str, str]:
        paths = [
            binary,
            temporal,
            Path(__file__),
            ROOT / "tools/runtime_artifact.py",
            args.core_prepared / "manifest.json",
            Path("/usr/bin/bwrap"),
            Path("/usr/sbin/runuser"),
            Path(sys.executable).resolve(),
            Path("/usr/lib/chromium/chromium"),
            Path("/usr/lib/x86_64-linux-gnu/libc.so.6"),
            Path("/usr/lib/x86_64-linux-gnu/libm.so.6"),
            Path("/usr/lib/x86_64-linux-gnu/libgcc_s.so.1"),
            Path("/usr/lib/x86_64-linux-gnu/ld-linux-x86-64.so.2"),
        ]
        paths += [ROOT / name for name in manifest["source_hashes"]]
        paths += [args.browser_source / name for name in browser_pin]

        def label(path: Path) -> str:
            if path == binary:
                return "native/core-runtime"
            if path == temporal:
                return "native/temporal"
            if path == args.core_prepared / "manifest.json":
                return "build/manifest.json"
            if path.is_relative_to(ROOT):
                return "core/" + str(path.relative_to(ROOT))
            if path.is_relative_to(args.browser_source):
                return "browser/" + str(path.relative_to(args.browser_source))
            return "host/" + str(path).lstrip("/")

        return {label(path): digest(path) for path in paths}

    before = input_hashes()
    check(
        all(
            digest(args.browser_source / name) == value
            for name, value in browser_pin.items()
        ),
        "Browser source pin mismatch",
    )
    cases = []
    for optimized in (False, True):
        for mode in ("after", "before"):
            with tempfile.TemporaryDirectory(prefix="core-chromium-temporal-") as tmp:
                os.chown(tmp, user.pw_uid, user.pw_gid)
                hosts = Path(tmp) / "hosts"
                hosts.write_text("127.0.0.1 localhost temporal\n")
                command = [
                    "runuser",
                    "-u",
                    args.user,
                    "--",
                    "bwrap",
                    "--unshare-net",
                    "--unshare-pid",
                    "--die-with-parent",
                    "--tmpfs",
                    "/",
                    "--ro-bind",
                    "/usr",
                    "/usr",
                    "--symlink",
                    "usr/bin",
                    "/bin",
                    "--symlink",
                    "usr/lib",
                    "/lib",
                    "--symlink",
                    "usr/lib64",
                    "/lib64",
                    "--ro-bind",
                    "/etc",
                    "/etc",
                    "--ro-bind",
                    str(hosts),
                    "/etc/hosts",
                    "--proc",
                    "/proc",
                    "--dev",
                    "/dev",
                    "--tmpfs",
                    "/tmp",
                    "--bind",
                    tmp,
                    "/effects",
                    "--ro-bind",
                    str(ROOT),
                    "/core-source",
                    "--ro-bind",
                    str(args.browser_source.resolve()),
                    "/browser-source",
                    "--ro-bind",
                    str(ROOT / "tests/fixtures"),
                    "/browser-fixture",
                    "--ro-bind",
                    str(binary.resolve()),
                    "/core-runtime",
                    "--ro-bind",
                    str(temporal.resolve()),
                    "/temporal",
                    "--chdir",
                    "/effects",
                    "--clearenv",
                    "--setenv",
                    "PATH",
                    "/usr/bin:/bin",
                    "--setenv",
                    "LANG",
                    "C.UTF-8",
                    "--setenv",
                    "PYTHONOPTIMIZE",
                    str(int(optimized)),
                    "--",
                    "/usr/bin/python3",
                    "-I",
                    "-B",
                    *(["-O"] if optimized else []),
                    "/core-source/tools/test_browser_chromium_temporal.py",
                    "--worker",
                    mode,
                    "--report",
                    "/effects/report.json",
                ]
                bounded(command)
                receipt = json.loads((Path(tmp) / "report.json").read_text())
                check(
                    receipt["optimization_level"] == int(optimized),
                    "worker optimization mismatch",
                )
                cases.append(receipt)
    check(input_hashes() == before, "native qualification inputs changed")
    report = {
        "input_hashes": before,
        "status": "passed",
        "evidence_class": "isolated_native_temporal_core_chromium_synthetic_site",
        "core_binary_sha256": digest(binary),
        "temporal_binary_sha256": digest(temporal),
        "core_source_hashes": manifest["source_hashes"],
        "cases": cases,
        "harness_sha256": digest(Path(__file__)),
        "host_runtime": {
            str(p): digest(p)
            for p in [
                Path(sys.executable).resolve(),
                Path("/usr/lib/chromium/chromium"),
                Path("/usr/lib/x86_64-linux-gnu/libc.so.6"),
                Path("/usr/lib/x86_64-linux-gnu/libm.so.6"),
                Path("/usr/lib/x86_64-linux-gnu/libgcc_s.so.1"),
                Path("/usr/lib/x86_64-linux-gnu/ld-linux-x86-64.so.2"),
            ]
        },
        "boundaries": {
            "site": "synthetic_loopback_http",
            "authority": "injected_synthetic",
            "browser_engine": "actual_pinned_sandboxed_Chromium",
            "runtime": "native_host_libraries_not_Docker_runtime_qualification",
            "credentials_profiles_live": "not_exercised",
            "helper_python_optimization": 0,
            "helper_timeout_seconds": 8,
            "activity_timeout_seconds": 10,
        },
    }
    args.report.write_text(json.dumps(report, indent=2) + "\n")
    print(json.dumps(report))


if __name__ == "__main__":
    main()
