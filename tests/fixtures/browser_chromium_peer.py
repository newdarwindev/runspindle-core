"""Pinned real renderer/Browser receiver with synthetic loopback site and authority."""

import argparse
import hashlib
import importlib
import importlib.util
import json
import os
import sqlite3
import sys
from contextlib import contextmanager
from pathlib import Path
from typing import Any, Iterator


def load_source(source: Path) -> None:
    pin = json.loads(
        Path(__file__).with_name("browser_chromium_source_pin.json").read_text()
    )["files"]
    for folder in ("browser_runtime", "browser_testing"):
        directory = source / folder
        found = {
            str(p.relative_to(source))
            for p in directory.rglob("*")
            if p.is_file() and "__pycache__" not in p.parts
        }
        expected = {name for name in pin if name.startswith(folder + "/")}
        if (
            directory.is_symlink()
            or found != expected
            or any(p.is_symlink() for p in directory.rglob("*"))
        ):
            raise ValueError("source inventory mismatch")
    for name, digest in pin.items():
        path = source / name
        if path.is_symlink() or hashlib.sha256(path.read_bytes()).hexdigest() != digest:
            raise ValueError("source bytes mismatch")
    sys.dont_write_bytecode = True
    sys.pycache_prefix = "/effects/browser-peer/unused-chromium-bytecode"
    for name in ("browser_runtime", "browser_testing"):
        directory = source / name
        spec = importlib.util.spec_from_file_location(
            name, directory / "__init__.py", submodule_search_locations=[str(directory)]
        )
        if spec is None or spec.loader is None:
            raise ValueError("source module unavailable")
        module = importlib.util.module_from_spec(spec)
        sys.modules[name] = module
        spec.loader.exec_module(module)
    # No unverified tools package initializer or sibling module lookup.
    import types

    tools = types.ModuleType("tools")
    tools.__path__ = []
    sys.modules["tools"] = tools
    spec = importlib.util.spec_from_file_location(
        "tools.browser_cdp", source / "tools/browser_cdp.py"
    )
    if spec is None or spec.loader is None:
        raise ValueError("CDP source unavailable")
    module = importlib.util.module_from_spec(spec)
    sys.modules["tools.browser_cdp"] = module
    spec.loader.exec_module(module)


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--source", type=Path, required=True)
    parser.add_argument("--root", type=Path, required=True)
    parser.add_argument("--action", choices=("dispatch", "reconcile"), required=True)
    args = parser.parse_args()
    load_source(args.source)
    credentials = importlib.import_module("browser_runtime.credentials")
    effects = importlib.import_module("browser_runtime.effects")
    guarded = importlib.import_module("browser_runtime.guarded_effects")
    wire = importlib.import_module("browser_runtime.effect_wire")
    engine_module = importlib.import_module("browser_testing.chromium_effect_site")
    http_module = importlib.import_module("browser_testing.http_effect_site")

    os.environ["RUNSPINDLE_CHROMIUM_EFFECT_TESTS"] = "1"
    engine_module.qualify()
    raw = sys.stdin.buffer.read(wire.MAX_REQUEST + 1)
    data = json.loads((args.root / "target.json").read_bytes())
    target = credentials.Target(
        effects.Binding(**data["binding"]),
        data["native_session"],
        data["frame"],
        data["frame_origin"],
    )
    payload = (args.root / "payload.bin").read_bytes()
    grant = guarded.Grant(
        target, hashlib.sha256(payload).hexdigest(), args.action, 20.0
    )
    ledger = sqlite3.connect(args.root / "transport.sqlite")
    ledger.executescript(
        "CREATE TABLE IF NOT EXISTS exchanges(action TEXT); CREATE TABLE IF NOT EXISTS calls(action TEXT);"
    )
    ledger.execute("INSERT INTO exchanges VALUES(?)", (args.action,))
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

    trusted = Trusted()
    with (
        http_module.LocalSite(args.root / "site.sqlite", submission="lost") as local,
        engine_module.ChromiumSite(local.port, target) as engine,
    ):

        class Site:
            def submit(self, value: Any, data: bytes, guard: Any) -> Any:
                if not trusted.current(value) or data != payload:
                    raise ValueError("synthetic grant denied")
                ledger.execute("INSERT INTO calls VALUES('submit')")
                ledger.commit()
                if (args.root / "crash-before-site").exists():
                    os._exit(94)
                return engine.submit(value, data, guard)

            def readback(self, value: Any, guard: Any) -> Any:
                if not trusted.current(value):
                    raise ValueError("synthetic grant denied")
                ledger.execute("INSERT INTO calls VALUES('readback')")
                ledger.commit()
                return engine.readback(value, guard)

        journal = guarded.GuardedJournal(
            args.root / "journal.sqlite",
            authority=trusted,
            site=Site(),
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
