"""Isolated fixture only: real Tether stores and wire parser, fake authority/HTTP.

Expected input is supplied independently by the Rust test before transport calls.
It is not a deployment credential and cannot select network endpoints. No socket
connection is permitted. This program does not ship as a production adapter.
"""

import argparse
import hashlib
import json
import os
import socket
import sys
from dataclasses import replace
from pathlib import Path
from unittest.mock import patch

from tests.test_admission import event, grant, proof
from tests.test_slack_upload import StoredHTTP
from tether.admission import Admission, Identity, canonical, pairs
from tether.testing import Attachment, Destination, Payload
from tether.wire_delivery import WireDeliveryJournal

VERSION = "runspindle.tether-wire-bridge.v1"


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--root", type=Path, required=True)
    parser.add_argument("--expected", type=Path, required=True)
    args = parser.parse_args()
    expected = json.loads(args.expected.read_bytes(), object_pairs_hook=pairs)
    raw = sys.stdin.buffer.read(3_145_729)
    if len(raw) > 3_145_728:
        raise ValueError("oversize request")
    request = json.loads(raw, object_pairs_hook=pairs)
    if (
        canonical(request) != raw
        or set(request) != {"version", "action", "envelope", "payload"}
        or request["version"] != VERSION
        or request["action"] not in {"send", "lookup"}
        or request["envelope"] != expected["envelope"]
        or request["payload"] != expected["payload"]
    ):
        raise ValueError("fixture request differs from independent trusted expectation")
    envelope = canonical(expected["envelope"])
    binding = expected["envelope"]["binding"]
    spec = expected["payload"]["attachment"]
    payload = Payload(
        expected["payload"]["text"],
        (
            Attachment(
                spec["attachment_id"],
                spec["filename"],
                bytes.fromhex(spec["content_hex"]),
            ),
        ),
    )
    identity = Identity("actor-1", "T100", "C100", "1700000000.000001")
    ingress = event(
        workspace_id="T100", channel_id="C100", thread_id=identity.thread_id
    )
    authority = replace(
        grant(ingress),
        identity=identity,
        task_id=binding["task_id"],
        revision=binding["revision"],
        operation_id=binding["operation_id"],
        policy_revision=binding["policy_revision"],
        host_id=binding["host_id"],
        lease_epoch=binding["lease_epoch"],
    )
    admission = Admission(
        str(args.root / "admission.db"),
        lambda value, signature: identity if signature == proof(value) else None,
        lambda *_: authority,
    )
    ingress_raw = canonical(ingress)
    admission.ingest(ingress_raw, proof(ingress_raw))
    fault = (args.root / "fault.txt").read_text()
    service = StoredHTTP(
        str(args.root / "service.db"), "" if fault == "lost:bridge-response" else fault
    )
    destination = Destination("T100", "C100", identity.thread_id)
    metadata = expected["envelope"]["payload"]

    def active():
        return not (args.root / "revoked").exists()

    journal = WireDeliveryJournal(
        str(args.root / "wire.db"),
        str(args.root / "journal.db"),
        service,
        admission,
        lambda value, digest: active()
        and value == envelope
        and digest == payload.digest(),
        lambda account, target: destination
        if (account, target) == (metadata["account_id"], metadata["destination_id"])
        else None,
        lambda _: active(),
    )
    try:
        result = getattr(journal, request["action"])(envelope, payload)
        if fault == "lost:bridge-response" and request["action"] == "send":
            os._exit(32)
        sys.stdout.buffer.write(
            canonical(
                {
                    "version": VERSION,
                    "request_sha256": hashlib.sha256(raw).hexdigest(),
                    "evidence_class": result.evidence_class,
                    "state": result.state,
                    "file_id": result.file_id,
                    "message_id": result.message_id,
                    "conflict": result.conflict,
                }
            )
        )
    finally:
        journal.close()
        service.close()
        admission.close()


if __name__ == "__main__":
    with patch.object(
        socket, "socket", side_effect=AssertionError("network forbidden")
    ):
        main()
