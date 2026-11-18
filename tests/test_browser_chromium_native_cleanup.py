"""Bounded wrapper cleanup; namespace probe is explicit and isolated."""

from __future__ import annotations

import json
import os
import pwd
import sys
import tempfile
import time
import unittest
from pathlib import Path

from tools.test_browser_chromium_temporal import bounded


def live_members(
    *, namespace: str | None = None, group: int | None = None
) -> list[int]:
    found = []
    for path in Path("/proc").glob("[0-9]*/stat"):
        try:
            fields = path.read_text().rsplit(")", 1)[1].split()
            if fields[0] == "Z":
                continue
            matches = (
                os.readlink(path.parent / "ns/pid") == namespace
                if namespace is not None
                else int(fields[2]) == group
            )
            if matches:
                found.append(int(path.parent.name))
        except (OSError, ValueError, IndexError):
            continue
    return found


class NativeWrapperCleanup(unittest.TestCase):
    def test_output_overflow_kills_owned_process_group(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            receipt = Path(tmp) / "owner.json"
            code = """import json,os,pathlib,subprocess,sys,time
subprocess.Popen([sys.executable,'-c','import time;time.sleep(60)'])
pathlib.Path(sys.argv[1]).write_text(json.dumps({'group':os.getpgrp()}))
sys.stdout.buffer.write(b'x'*1100000);sys.stdout.buffer.flush();time.sleep(60)
"""
            with self.assertRaisesRegex(RuntimeError, "output overflow"):
                bounded(
                    [sys.executable, "-I", "-B", "-c", code, str(receipt)], timeout=5
                )
            group = json.loads(receipt.read_text())["group"]
            deadline = time.monotonic() + 5
            while live_members(group=group) and time.monotonic() < deadline:
                time.sleep(0.025)
            self.assertEqual(live_members(group=group), [])

    @unittest.skipUnless(
        os.geteuid() == 0
        and os.environ.get("RUNSPINDLE_NATIVE_NAMESPACE_TESTS") == "1",
        "requires root and explicit isolated namespace opt-in",
    )
    def test_timeout_removes_entire_owned_pid_namespace(self) -> None:
        user = pwd.getpwnam("standard")
        self.assertNotEqual(user.pw_uid, 0)
        with tempfile.TemporaryDirectory(prefix="native-cleanup-") as tmp:
            os.chown(tmp, user.pw_uid, user.pw_gid)
            code = """import json,os,pathlib,subprocess,sys,time
if os.geteuid()==0: raise RuntimeError('root worker')
interfaces={line.split(':',1)[0].strip() for line in pathlib.Path('/proc/self/net/dev').read_text().splitlines() if ':' in line}
if interfaces!={'lo'}: raise RuntimeError('not isolated')
subprocess.Popen([sys.executable,'-c','import time;time.sleep(60)'],start_new_session=True)
pathlib.Path('/effects/namespace.json').write_text(json.dumps({'namespace':os.readlink('/proc/self/ns/pid'),'uid':os.geteuid()}))
time.sleep(60)
"""
            command = [
                "runuser",
                "-u",
                "standard",
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
                "--proc",
                "/proc",
                "--dev",
                "/dev",
                "--tmpfs",
                "/tmp",
                "--bind",
                tmp,
                "/effects",
                "--chdir",
                "/effects",
                "--clearenv",
                "--setenv",
                "PATH",
                "/usr/bin:/bin",
                "--",
                "/usr/bin/python3",
                "-I",
                "-B",
                "-c",
                code,
            ]
            with self.assertRaisesRegex(RuntimeError, "deadline"):
                bounded(command, timeout=3)
            receipt = json.loads((Path(tmp) / "namespace.json").read_text())
            self.assertEqual(receipt["uid"], user.pw_uid)
            namespace = receipt["namespace"]
            self.assertNotEqual(namespace, os.readlink("/proc/self/ns/pid"))
            deadline = time.monotonic() + 5
            while live_members(namespace=namespace) and time.monotonic() < deadline:
                time.sleep(0.025)
            self.assertEqual(live_members(namespace=namespace), [])


if __name__ == "__main__":
    unittest.main()
