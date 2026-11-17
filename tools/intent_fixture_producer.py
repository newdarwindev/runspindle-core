#!/usr/bin/env python3
"""Explicit synthetic HMAC producer for the disposable semantic-journal lane."""

from __future__ import annotations

import argparse
import hashlib
import hmac
import os
import sys
from typing import Any


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--tether", required=True)
    parser.add_argument("--database", required=True)
    parser.add_argument(
        "--action",
        required=True,
        choices=["retain-1", "retain-2", "retain-3", "snapshot", "crash-after-commit"],
    )
    args = parser.parse_args()
    sys.path.insert(0, args.tether)
    from tether.admission import (  # type: ignore[import-not-found]
        Grant,
        Identity,
        canonical,
    )
    from tether.intent import expected_task_id  # type: ignore[import-not-found]
    from tether.semantic_journal import (  # type: ignore[import-not-found]
        IntentReferences,
        SemanticAdmission,
    )

    source = dict(
        app_id="app-1",
        account_id="account-1",
        actor_id="synthetic-actor",
        workspace_id="workspace-1",
        channel_id="channel-1",
        thread_id="thread-1",
        message_id="message-journal",
    )
    identity = Identity(
        source["actor_id"],
        source["workspace_id"],
        source["channel_id"],
        source["thread_id"],
    )
    task = expected_task_id(source)
    allowed: dict[str, Any] = {}
    secret = b"public-synthetic-transport-fixture-not-a-real-key"

    def authenticate(raw: bytes, proof: object) -> Any:
        return (
            identity
            if type(proof) is bytes
            and hmac.compare_digest(hmac.digest(secret, raw, "sha256"), proof)
            else None
        )

    def refs(envelope: dict[str, Any]) -> Any:
        revision = envelope["binding"]["revision"]
        return IntentReferences(
            f"grant-journal-{revision}", f"delivery-journal-{revision}"
        )

    app = SemanticAdmission(
        args.database,
        app_id="app-1",
        account_id="account-1",
        references=refs,
        authenticate=authenticate,
        authorize=lambda actor, digest: allowed.get(digest),
    )
    try:
        if args.action != "snapshot":
            revision = (
                1 if args.action == "crash-after-commit" else int(args.action[-1])
            )
            kind = {1: "create", 2: "edit", 3: "delete"}[revision]
            raw = canonical(
                dict(
                    version="runspindle.tether.ingress.v1",
                    event_id=f"event-journal-{revision}",
                    message_id=source["message_id"],
                    actor_id=source["actor_id"],
                    workspace_id=source["workspace_id"],
                    channel_id=source["channel_id"],
                    thread_id=source["thread_id"],
                    revision=revision,
                    kind=kind,
                    text="" if kind == "delete" else f"journal-content-{revision}",
                    mirrored=False,
                )
            )
            digest = hashlib.sha256(raw).hexdigest()
            allowed[digest] = Grant(
                identity,
                digest,
                task,
                revision,
                f"op-journal-{revision}",
                "policy-1",
                "host-1",
                1,
                f"legacy-journal-{revision}",
                "intent-test",
            )
            result = app.ingest(raw, hmac.digest(secret, raw, "sha256"))
            if result not in ("admitted", "duplicate"):
                raise AssertionError("synthetic local admission")
            if args.action == "crash-after-commit":
                os._exit(91)
        sys.stdout.buffer.write(app.snapshot_batch(task) + b"\n")
    finally:
        app.close()


if __name__ == "__main__":
    main()
