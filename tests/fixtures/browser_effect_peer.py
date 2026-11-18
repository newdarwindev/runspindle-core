"""Explicit synthetic fixture: pinned real Browser receiver, independent SQL site.

No production launcher, ticket issuer, browser, network or credentials. Trusted
expectations are separate files written before the Core client sends any bytes.
"""

import argparse
import hashlib
import importlib
import importlib.util
import json
import socket
import sqlite3
import sys
from contextlib import contextmanager
from pathlib import Path
from typing import Any, Iterator
from unittest.mock import patch


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--source", type=Path, required=True)
    parser.add_argument("--root", type=Path, required=True)
    parser.add_argument("--action", choices=["dispatch", "reconcile"], required=True)
    args = parser.parse_args()
    pin = json.loads(Path(__file__).with_name("browser_source_pin.json").read_text())
    package = args.source / "browser_runtime"
    found = {str(p.relative_to(args.source)): p for p in package.rglob("*.py")}
    if (
        package.is_symlink()
        or any(p.is_symlink() for p in package.rglob("*"))
        or any(
            p.is_file()
            and "__pycache__" not in p.relative_to(package).parts
            and str(p.relative_to(args.source)) not in pin["files"]
            for p in package.rglob("*")
        )
        or set(found) != set(pin["files"])
    ):
        raise ValueError("browser package inventory mismatch")
    for name, path in found.items():
        if (
            path.is_symlink()
            or hashlib.sha256(path.read_bytes()).hexdigest() != pin["files"][name]
        ):
            raise ValueError("browser package bytes mismatch")
    # Do not consume ignored bytecode caches from the external checkout.
    sys.pycache_prefix = str(args.root / "unused-bytecode-cache")
    sys.dont_write_bytecode = True
    spec = importlib.util.spec_from_file_location(
        "browser_runtime",
        package / "__init__.py",
        submodule_search_locations=[str(package)],
    )
    if spec is None or spec.loader is None:
        raise ValueError("browser package unavailable")
    module = importlib.util.module_from_spec(spec)
    sys.modules["browser_runtime"] = module
    spec.loader.exec_module(module)
    credentials = importlib.import_module("browser_runtime.credentials")
    effects = importlib.import_module("browser_runtime.effects")
    guarded = importlib.import_module("browser_runtime.guarded_effects")
    wire = importlib.import_module("browser_runtime.effect_wire")
    target = json.loads((args.root / "target.json").read_bytes())
    target = credentials.Target(
        effects.Binding(**target["binding"]),
        target["native_session"],
        target["frame"],
        target["frame_origin"],
    )
    payload = (args.root / "payload.bin").read_bytes()
    digest = hashlib.sha256(payload).hexdigest()
    grant = guarded.Grant(target, digest, args.action, 20.0)
    ledger = sqlite3.connect(args.root / "site.sqlite")
    ledger.executescript(
        "CREATE TABLE IF NOT EXISTS calls(action TEXT); CREATE TABLE IF NOT EXISTS effects(operation TEXT PRIMARY KEY,payload BLOB NOT NULL);"
    )
    ledger.commit()

    class Trusted:
        active = False

        @contextmanager
        def acquire(self) -> Iterator[Any]:
            self.active = True
            try:
                yield self
            finally:
                self.active = False

        def admit(self, ticket: object) -> Any:
            return grant if ticket == "synthetic-approved" else None

        def observe(self) -> Any:
            return credentials.Observation(target, 10.0)

        def current(self, value: object) -> bool:
            return (
                self.active and value == grant and not (args.root / "revoked").exists()
            )

        def submit(self, value: Any, data: bytes, guard: Any) -> object:
            if not self.current(value) or guard is not self or data != payload:
                raise ValueError("invalid trusted submission")
            ledger.execute("INSERT INTO calls VALUES('submit')")
            ledger.execute(
                "INSERT INTO effects VALUES(?,?)", (target.binding.operation, data)
            )
            ledger.commit()
            raise RuntimeError("synthetic accepted response lost")

        def readback(self, value: Any, guard: Any) -> Any:
            if not self.current(value) or guard is not self:
                raise ValueError("invalid trusted observation")
            ledger.execute("INSERT INTO calls VALUES('readback')")
            ledger.commit()
            row = ledger.execute(
                "SELECT payload FROM effects WHERE operation=?",
                (target.binding.operation,),
            ).fetchone()
            if row != (payload,) or (args.root / "unresolved").exists():
                raise ValueError("observation unavailable")
            return effects.Readback(
                target.binding,
                digest,
                "confirmed",
                hashlib.sha256(b"independent-site-evidence" + payload).hexdigest(),
            )

    trusted = Trusted()
    raw = sys.stdin.buffer.read(wire.MAX_REQUEST + 1)
    with (
        patch.object(
            socket.socket, "connect", side_effect=AssertionError("network forbidden")
        ),
        patch.object(
            socket, "create_connection", side_effect=AssertionError("network forbidden")
        ),
    ):
        journal = guarded.GuardedJournal(
            args.root / "journal.sqlite",
            authority=trusted,
            site=trusted,
            clock=lambda: 10.0,
        )
        try:
            result = wire.Receiver(journal).exchange(raw, "synthetic-approved")
        finally:
            journal.close()
            ledger.close()
    sys.stdout.buffer.write(result)


if __name__ == "__main__":
    main()
