"""Canonical event envelope helpers for Arcane Arcade stage contracts."""

from __future__ import annotations

import datetime as dt
import uuid
from typing import Any, Dict, Iterable, Optional

EVENT_SCHEMA_VERSION = "v1"

# Required event taxonomy for Sprint0 contract migration.
REQUIRED_EVENT_TYPES = {
    "stage.claim.requested",
    "stage.claim.granted",
    "stage.work.started",
    "stage.work.heartbeat",
    "stage.work.completed",
    "stage.work.failed",
    "stage.work.skipped",
    "task.state.changed",
    "review.requested",
    "review.completed",
    "scrum.blocker.raised",
    "scrum.blocker.cleared",
    "scrum.user_action.required",
    "worker.exhaustion.updated",
    "sprint.health.updated",
}

ALLOWED_STATUSES = {
    "received",
    "started",
    "completed",
    "failed",
    "skipped",
    "blocked",
    "ok",
}


class EventSchemaValidationError(ValueError):
    """Raised when an event envelope does not satisfy contract requirements."""


def utc_now_iso() -> str:
    # Canonical events use UTC timestamps so replay and cross-stage ordering stay consistent.
    # This avoids mixed local-time payloads when different worker processes emit events.
    # Keep formatting centralized so every envelope uses the same timestamp representation.
    return dt.datetime.now(tz=dt.timezone.utc).isoformat()


def make_idempotency_key(aggregate_id: str, stage: str, attempt: int) -> str:
    """Build deterministic idempotency key for projection dedupe."""
    return f"{aggregate_id}:{stage}:{attempt}"


def _coerce_list(values: Optional[Iterable[str]]) -> list[str]:
    # Artifact refs may arrive as tuples, generators, or None from caller code.
    # Normalize once here so downstream validators operate on one concrete shape.
    # This keeps envelope construction simple and avoids repeated conversion logic.
    if values is None:
        return []
    return [str(v) for v in values]


def validate_event_envelope(payload: Dict[str, Any]) -> None:
    # Validation must fail fast because projections depend on strict field presence.
    # Rejecting bad envelopes at creation time is cheaper than repairing bad replay state later.
    # Keep taxonomy and status checks centralized so transport adapters stay dumb.
    required_fields = (
        "event_id",
        "event_type",
        "ts_utc",
        "correlation_id",
        "causation_id",
        "aggregate_type",
        "aggregate_id",
        "stage",
        "worker",
        "attempt",
        "idempotency_key",
        "status",
        "reason",
        "requires_user_action",
        "artifact_refs",
        "metrics",
        "payload",
        "schema_version",
    )
    missing = [field for field in required_fields if field not in payload]
    if missing:
        raise EventSchemaValidationError(f"missing required event fields: {', '.join(missing)}")

    event_type = str(payload.get("event_type", "")).strip()
    if event_type not in REQUIRED_EVENT_TYPES:
        raise EventSchemaValidationError(f"unsupported event_type: {event_type}")

    status = str(payload.get("status", "")).strip().lower()
    if status not in ALLOWED_STATUSES:
        raise EventSchemaValidationError(f"unsupported status: {status}")

    attempt = payload.get("attempt", 0)
    if not isinstance(attempt, int) or attempt < 1:
        raise EventSchemaValidationError("attempt must be a positive integer")

    for key in ("event_id", "correlation_id", "aggregate_type", "aggregate_id", "stage", "worker", "idempotency_key"):
        if not str(payload.get(key, "")).strip():
            raise EventSchemaValidationError(f"{key} cannot be empty")


# Central constructor keeps all stage events schema-consistent.
def build_event_envelope(
    *,
    event_type: str,
    correlation_id: str,
    causation_id: str,
    aggregate_type: str,
    aggregate_id: str,
    stage: str,
    worker: str,
    attempt: int,
    idempotency_key: str,
    status: str,
    reason: str = "",
    requires_user_action: bool = False,
    artifact_refs: Optional[Iterable[str]] = None,
    metrics: Optional[Dict[str, Any]] = None,
    payload: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    # Every canonical event is assembled through one constructor so schema drift is contained.
    # Callers provide business context while this helper handles ids, defaults, and normalization.
    # Validation runs before return so invalid payloads never reach the event stream.
    envelope: Dict[str, Any] = {
        "event_id": str(uuid.uuid4()),
        "event_type": str(event_type),
        "ts_utc": utc_now_iso(),
        "correlation_id": str(correlation_id),
        "causation_id": str(causation_id),
        "aggregate_type": str(aggregate_type),
        "aggregate_id": str(aggregate_id),
        "stage": str(stage),
        "worker": str(worker),
        "attempt": int(attempt),
        "idempotency_key": str(idempotency_key),
        "status": str(status).strip().lower(),
        "reason": str(reason),
        "requires_user_action": bool(requires_user_action),
        "artifact_refs": _coerce_list(artifact_refs),
        "metrics": dict(metrics or {}),
        "payload": dict(payload or {}),
        "schema_version": EVENT_SCHEMA_VERSION,
    }
    validate_event_envelope(envelope)
    return envelope
