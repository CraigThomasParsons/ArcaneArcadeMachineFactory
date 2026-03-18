#!/usr/bin/env python3
"""Local WebSocket broker for Arcane Arcade stage/chatroom events.

Protocol:
- Client sends JSON messages.
- Envelope command types:
  - {"cmd":"subscribe", "channel":"stage.chatroom", "last_seen_event_id":"..."}
  - {"cmd":"publish", "channel":"stage.chatroom", "event":{...}}
  - {"cmd":"ping"}
- Broker responses:
  - {"type":"ack", "cmd":"subscribe", "channel":"..."}
  - {"type":"event", "channel":"...", "event":{...}}
  - {"type":"pong"}
  - {"type":"error", "message":"..."}

For MVP, broker keeps a short in-memory replay buffer per channel and appends
published events to a JSONL audit log.
"""

from __future__ import annotations

import argparse
import asyncio
import json
from collections import defaultdict, deque
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import websockets
from websockets.server import WebSocketServerProtocol


@dataclass
class BrokerConfig:
    host: str
    port: int
    replay_limit: int
    audit_dir: Path


class EventBroker:
    def __init__(self, config: BrokerConfig) -> None:
        self._config = config
        self._subscriptions: dict[str, set[WebSocketServerProtocol]] = defaultdict(set)
        self._history: dict[str, deque[dict[str, Any]]] = defaultdict(
            lambda: deque(maxlen=config.replay_limit)
        )

    async def handler(self, websocket: WebSocketServerProtocol) -> None:
        try:
            async for raw in websocket:
                await self._handle_message(websocket, raw)
        except websockets.ConnectionClosed:
            pass
        finally:
            self._remove_socket(websocket)

    async def _handle_message(self, websocket: WebSocketServerProtocol, raw: str) -> None:
        try:
            msg = json.loads(raw)
        except json.JSONDecodeError:
            await self._send(websocket, {"type": "error", "message": "invalid json"})
            return

        cmd = msg.get("cmd")
        if cmd == "subscribe":
            await self._subscribe(websocket, msg)
            return
        if cmd == "publish":
            await self._publish(msg)
            return
        if cmd == "ping":
            await self._send(websocket, {"type": "pong"})
            return

        await self._send(websocket, {"type": "error", "message": "unknown cmd"})

    async def _subscribe(self, websocket: WebSocketServerProtocol, msg: dict[str, Any]) -> None:
        channel = str(msg.get("channel", "")).strip()
        if not channel:
            await self._send(websocket, {"type": "error", "message": "missing channel"})
            return

        self._subscriptions[channel].add(websocket)
        await self._send(websocket, {"type": "ack", "cmd": "subscribe", "channel": channel})

        # Replay channel history after last_seen_event_id when provided.
        last_seen = str(msg.get("last_seen_event_id", "")).strip()
        started = last_seen == ""
        for envelope in self._history[channel]:
            event_id = str(envelope.get("event", {}).get("event_id", ""))
            if not started:
                if event_id == last_seen:
                    started = True
                continue
            await self._send(websocket, envelope)

    async def _publish(self, msg: dict[str, Any]) -> None:
        channel = str(msg.get("channel", "")).strip()
        event = msg.get("event")
        if not channel or not isinstance(event, dict):
            return

        now = datetime.now(timezone.utc).isoformat()
        if not event.get("ts"):
            event["ts"] = now
        if not event.get("schema_version"):
            event["schema_version"] = "v1"
        if not event.get("event_id"):
            event["event_id"] = f"{channel}-{int(datetime.now().timestamp() * 1000)}"

        envelope = {"type": "event", "channel": channel, "event": event}
        self._history[channel].append(envelope)
        self._append_audit(channel, event)

        for socket in list(self._subscriptions[channel]):
            if socket.closed:
                self._subscriptions[channel].discard(socket)
                continue
            await self._send(socket, envelope)

    def _append_audit(self, channel: str, event: dict[str, Any]) -> None:
        self._config.audit_dir.mkdir(parents=True, exist_ok=True)
        safe_channel = channel.replace("/", "_")
        path = self._config.audit_dir / f"{safe_channel}.jsonl"
        with path.open("a", encoding="utf-8") as handle:
            handle.write(json.dumps(event, ensure_ascii=True) + "\n")

    async def _send(self, websocket: WebSocketServerProtocol, payload: dict[str, Any]) -> None:
        try:
            await websocket.send(json.dumps(payload, ensure_ascii=True))
        except websockets.ConnectionClosed:
            self._remove_socket(websocket)

    def _remove_socket(self, websocket: WebSocketServerProtocol) -> None:
        for channel in list(self._subscriptions.keys()):
            self._subscriptions[channel].discard(websocket)
            if not self._subscriptions[channel]:
                del self._subscriptions[channel]


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Arcane Arcade WebSocket broker")
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--port", type=int, default=8765)
    parser.add_argument("--replay-limit", type=int, default=500)
    parser.add_argument(
        "--audit-dir",
        default="/home/craigpar/Code/ArcaneArcadeMachineFactory/TheFactoryHopper/chatroom_ws_audit",
    )
    return parser


async def main_async() -> None:
    args = build_parser().parse_args()
    config = BrokerConfig(
        host=args.host,
        port=args.port,
        replay_limit=args.replay_limit,
        audit_dir=Path(args.audit_dir),
    )

    broker = EventBroker(config)
    async with websockets.serve(broker.handler, config.host, config.port):
        print(f"[ws-broker] listening on ws://{config.host}:{config.port}")
        await asyncio.Future()


if __name__ == "__main__":
    asyncio.run(main_async())
