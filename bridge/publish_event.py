#!/usr/bin/env python3
"""Simple agent-side publisher for Arcane Arcade WebSocket broker."""

from __future__ import annotations

import argparse
import asyncio
import json
import uuid
from datetime import datetime, timezone
from pathlib import Path

import websockets


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Publish one chat event")
    parser.add_argument("--broker", default="ws://127.0.0.1:8765")
    parser.add_argument("--channel", required=True)
    parser.add_argument("--stage", required=True)
    parser.add_argument("--actor", required=True)
    parser.add_argument("--event", required=True)
    parser.add_argument("--summary", required=True)
    parser.add_argument("--status", default="ok", choices=["ok", "warn", "failed"])
    parser.add_argument("--error-code", default="")
    parser.add_argument("--artifact-ref", action="append", default=[])
    parser.add_argument("--payload-file", default="")
    return parser


def load_payload(path: str) -> dict | None:
    if not path:
        return None
    raw = Path(path).read_text(encoding="utf-8")
    return json.loads(raw)


async def main_async() -> None:
    args = build_parser().parse_args()

    event = {
        "event_id": str(uuid.uuid4()),
        "ts": datetime.now(timezone.utc).isoformat(),
        "stage": args.stage,
        "actor": args.actor,
        "event": args.event,
        "summary": args.summary,
        "artifact_refs": args.artifact_ref,
        "status": args.status,
        "error_code": args.error_code,
        "schema_version": "v1",
        "payload": load_payload(args.payload_file),
    }

    envelope = {"cmd": "publish", "channel": args.channel, "event": event}

    async with websockets.connect(args.broker) as socket:
        await socket.send(json.dumps(envelope, ensure_ascii=True))


if __name__ == "__main__":
    asyncio.run(main_async())
