"""Publisher abstraction for Arcane canonical event transport."""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any, Dict, Protocol


class EventPublishError(RuntimeError):
    """Raised when event publishing fails with retry metadata."""

    def __init__(self, message: str, *, transient: bool) -> None:
        self.transient = transient
        super().__init__(message)


class EventPublisher(Protocol):
    def publish(self, payload: Dict[str, Any]) -> None:
        ...

    def describe(self) -> str:
        ...


class FileEventPublisher:
    def __init__(self, event_stream_path: Path) -> None:
        self._event_stream_path = event_stream_path

    def publish(self, payload: Dict[str, Any]) -> None:
        # File publishing is the safe default during Sprint0 migration.
        # Append-only writes preserve event ordering for deterministic replay.
        # Any filesystem failure is treated as terminal for this adapter.
        try:
            self._event_stream_path.parent.mkdir(parents=True, exist_ok=True)
            with self._event_stream_path.open("a", encoding="utf-8") as handle:
                handle.write(json.dumps(payload, ensure_ascii=True) + "\n")
        except OSError as exc:
            raise EventPublishError(f"file publisher write failed: {exc}", transient=False) from exc

    def describe(self) -> str:
        return f"file:{self._event_stream_path}"


class TransportPlaceholderPublisher:
    def __init__(self, transport_name: str) -> None:
        self._transport_name = transport_name

    def publish(self, payload: Dict[str, Any]) -> None:
        # Placeholder publishers make the rollout path explicit without pretending transport exists.
        # Marking the failure as transient leaves room for retry policy once real adapters land.
        # This keeps unsupported transport selection visible during early integration work.
        raise EventPublishError(
            f"transport publisher not implemented: {self._transport_name}",
            transient=True,
        )

    def describe(self) -> str:
        return f"placeholder:{self._transport_name}"


def build_event_publisher(*, publisher_type: str, event_stream_path: Path) -> EventPublisher:
    # Resolve publisher type once so runtime code does not branch on transport details.
    # Sprint0 defaults to file, but the factory can opt into future durable bus adapters here.
    # Unknown types fail immediately so misconfiguration is obvious at startup or smoke time.
    normalized = publisher_type.strip().lower()
    if normalized in {"file", "jsonl"}:
        return FileEventPublisher(event_stream_path)
    if normalized in {"redis_streams", "nats_jetstream"}:
        return TransportPlaceholderPublisher(normalized)
    raise EventPublishError(f"unsupported ARCANE_EVENT_PUBLISHER: {publisher_type}", transient=False)
