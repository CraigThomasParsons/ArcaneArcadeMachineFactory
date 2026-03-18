"""Projection builders for Arcane canonical event streams."""

from __future__ import annotations

import datetime as dt
import json
from pathlib import Path
from typing import Any, Dict, List

PROJECTION_SCHEMA_VERSION = "v1"


def _utc_now_iso() -> str:
    return dt.datetime.now(tz=dt.timezone.utc).isoformat()


def _safe_parse_ts(ts_value: Any) -> dt.datetime:
    # Projection age metrics must handle malformed timestamps without crashing replay.
    # Invalid values are coerced to epoch so age calculations still produce deterministic output.
    # This keeps the rebuild command robust on partially-corrupted historical events.
    ts_text = str(ts_value or "").strip()
    if not ts_text:
        return dt.datetime.fromtimestamp(0, tz=dt.timezone.utc)
    try:
        if ts_text.endswith("Z"):
            ts_text = ts_text[:-1] + "+00:00"
        parsed = dt.datetime.fromisoformat(ts_text)
        if parsed.tzinfo is None:
            parsed = parsed.replace(tzinfo=dt.timezone.utc)
        return parsed.astimezone(dt.timezone.utc)
    except Exception:
        return dt.datetime.fromtimestamp(0, tz=dt.timezone.utc)


def load_event_stream_events(event_stream_path: Path, *, start_line: int = 1, max_events: int = 0) -> List[Dict[str, Any]]:
    # Stream loader supports replay-range workflows after interrupted runs.
    # Invalid JSON lines are skipped so one malformed record does not block recovery.
    # Line metadata is attached to each event for integrity diagnostics and traceability.
    if not event_stream_path.exists():
        return []

    bounded_start_line = max(1, int(start_line))
    bounded_max_events = max(0, int(max_events))

    events: List[Dict[str, Any]] = []
    for line_number, line in enumerate(event_stream_path.read_text(encoding="utf-8").splitlines(), start=1):
        if line_number < bounded_start_line:
            continue
        if not line.strip():
            continue
        try:
            payload = json.loads(line)
        except Exception:
            continue
        if not isinstance(payload, dict):
            continue

        event_payload = dict(payload)
        event_payload["_stream_line"] = line_number
        events.append(event_payload)

        if bounded_max_events and len(events) >= bounded_max_events:
            break
    return events


def _blocker_key(event: Dict[str, Any]) -> str:
    payload = event.get("payload", {}) if isinstance(event.get("payload"), dict) else {}
    blocker_key = str(payload.get("blocker_key", "")).strip()
    if blocker_key:
        return blocker_key

    stage_name = str(event.get("stage", ""))
    aggregate_id = str(event.get("aggregate_id", ""))
    reason = str(event.get("reason", ""))
    return f"{stage_name}:{aggregate_id}:{reason}"


def build_projection_state_from_events(events: List[Dict[str, Any]], *, source_stream: Path, source_filters: Dict[str, Any]) -> Dict[str, Any]:
    # Replay from the canonical stream start ensures deterministic projection rebuilds.
    # We dedupe by idempotency key so duplicate deliveries do not corrupt read models.
    # Every read model branch below is driven by immutable event facts only.
    seen_idempotency_keys: set[str] = set()
    blockers: Dict[str, Dict[str, Any]] = {}
    tasks: Dict[str, Dict[str, Any]] = {}
    workers: Dict[str, Dict[str, Any]] = {}

    throughput = {
        "completed_count": 0,
        "failed_count": 0,
        "skipped_count": 0,
    }

    for event in events:
        idempotency_key = str(event.get("idempotency_key", "")).strip()
        if idempotency_key and idempotency_key in seen_idempotency_keys:
            continue
        if idempotency_key:
            seen_idempotency_keys.add(idempotency_key)

        event_type = str(event.get("event_type", "")).strip()
        aggregate_id = str(event.get("aggregate_id", "")).strip()
        stage_name = str(event.get("stage", "")).strip()
        worker_name = str(event.get("worker", "")).strip() or "unknown-worker"
        attempt = int(event.get("attempt", 1) or 1)
        event_ts = str(event.get("ts_utc", ""))
        event_reason = str(event.get("reason", "")).strip()
        event_status = str(event.get("status", "")).strip()

        if aggregate_id:
            task_key = aggregate_id
            task_state = tasks.get(task_key, {"attempts": {}})
            attempts = task_state.get("attempts", {})
            attempt_key = str(attempt)
            current_attempt = attempts.get(attempt_key, {})
            current_attempt.update(
                {
                    "stage": stage_name,
                    "last_event_type": event_type,
                    "last_status": event_status,
                    "last_reason": event_reason,
                    "updated_at": event_ts,
                }
            )

            if event_type in {"stage.work.completed", "stage.work.failed", "stage.work.skipped"}:
                current_attempt["terminal_event_type"] = event_type
                current_attempt["terminal_status"] = event_status

            attempts[attempt_key] = current_attempt
            task_state["attempts"] = attempts
            task_state["latest_attempt"] = attempt
            task_state["updated_at"] = event_ts
            tasks[task_key] = task_state

        if event_type == "worker.exhaustion.updated":
            payload = event.get("payload", {}) if isinstance(event.get("payload"), dict) else {}
            exhaustion_value = float(payload.get("exhaustion", 0.0) or 0.0)
            workers[worker_name] = {
                "worker": worker_name,
                "exhaustion": exhaustion_value,
                "source": "explicit_event",
                "updated_at": event_ts,
            }

        if event_type in {"stage.work.completed", "stage.work.failed", "stage.work.skipped"}:
            # When explicit exhaustion events are absent, keep a coarse projection input model.
            # This lets operators spot load hotspots before full exhaustion modeling is added.
            # Values are intentionally conservative and can be replaced by explicit events later.
            if worker_name not in workers:
                workers[worker_name] = {
                    "worker": worker_name,
                    "exhaustion": 0.0,
                    "source": "derived_work_events",
                    "updated_at": event_ts,
                }

            increment = 0.0
            if event_type == "stage.work.completed":
                increment = 0.05
                throughput["completed_count"] += 1
            elif event_type == "stage.work.failed":
                increment = 0.15
                throughput["failed_count"] += 1
            elif event_type == "stage.work.skipped":
                increment = 0.08
                throughput["skipped_count"] += 1

            workers[worker_name]["exhaustion"] = min(1.0, float(workers[worker_name].get("exhaustion", 0.0)) + increment)
            workers[worker_name]["updated_at"] = event_ts

        if event_type == "scrum.blocker.raised":
            blocker_key = _blocker_key(event)
            payload = event.get("payload", {}) if isinstance(event.get("payload"), dict) else {}
            blockers[blocker_key] = {
                "blocker_key": blocker_key,
                "blocker_id": str(payload.get("blocker_id", "")).strip(),
                "stage": stage_name,
                "job_id": aggregate_id,
                "reason": event_reason,
                "status": "open",
                "requires_user_action": bool(event.get("requires_user_action", False)),
                "opened_at": event_ts,
                "updated_at": event_ts,
            }

        if event_type == "scrum.user_action.required":
            blocker_key = _blocker_key(event)
            existing = blockers.get(blocker_key, {"blocker_key": blocker_key})
            existing["requires_user_action"] = True
            existing["updated_at"] = event_ts
            blockers[blocker_key] = existing

        if event_type == "scrum.blocker.cleared":
            blocker_key = _blocker_key(event)
            existing = blockers.get(blocker_key, {"blocker_key": blocker_key})
            existing["status"] = "cleared"
            existing["cleared_at"] = event_ts
            existing["updated_at"] = event_ts
            blockers[blocker_key] = existing

    open_blockers = []
    for blocker in blockers.values():
        if str(blocker.get("status", "")) != "open":
            continue
        opened_at = _safe_parse_ts(blocker.get("opened_at", ""))
        age_seconds = max(0.0, (dt.datetime.now(tz=dt.timezone.utc) - opened_at).total_seconds())
        item = dict(blocker)
        item["age_seconds"] = int(age_seconds)
        open_blockers.append(item)

    open_blockers.sort(key=lambda blocker: int(blocker.get("age_seconds", 0)), reverse=True)

    active_task_count = 0
    for task_state in tasks.values():
        latest_attempt = str(task_state.get("latest_attempt", ""))
        attempt_state = task_state.get("attempts", {}).get(latest_attempt, {})
        terminal_event = str(attempt_state.get("terminal_event_type", ""))
        if terminal_event not in {"stage.work.completed", "stage.work.failed", "stage.work.skipped"}:
            active_task_count += 1

    sprint_health_status = "ok"
    if any(bool(blocker.get("requires_user_action", False)) for blocker in open_blockers):
        sprint_health_status = "critical"
    elif open_blockers or throughput["failed_count"] > 0:
        sprint_health_status = "warning"

    state = {
        "schema_version": PROJECTION_SCHEMA_VERSION,
        "generated_at": _utc_now_iso(),
        "source": {
            "event_stream": str(source_stream),
            "event_count": len(events),
            "deduped_event_count": len(seen_idempotency_keys),
            "filters": source_filters,
        },
        "blockers": {
            "open": open_blockers,
            "all": blockers,
        },
        "tasks": {
            "items": tasks,
            "active_task_count": active_task_count,
        },
        "workers": {
            "items": workers,
        },
        "sprint": {
            "health_status": sprint_health_status,
            "open_blocker_count": len(open_blockers),
            "active_task_count": active_task_count,
            "throughput": throughput,
        },
    }
    return state


def build_projection_state(event_stream_path: Path) -> Dict[str, Any]:
    events = load_event_stream_events(event_stream_path)
    return build_projection_state_from_events(
        events,
        source_stream=event_stream_path,
        source_filters={"start_line": 1, "max_events": 0},
    )


def write_projection_state(output_path: Path, payload: Dict[str, Any]) -> None:
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(json.dumps(payload, indent=2, ensure_ascii=True) + "\n", encoding="utf-8")


def load_projection_state(path: Path) -> Dict[str, Any]:
    if not path.exists():
        return {
            "schema_version": PROJECTION_SCHEMA_VERSION,
            "generated_at": "",
            "source": {"event_stream": "", "event_count": 0, "deduped_event_count": 0},
            "blockers": {"open": [], "all": {}},
            "tasks": {"items": {}, "active_task_count": 0},
            "workers": {"items": {}},
            "sprint": {
                "health_status": "ok",
                "open_blocker_count": 0,
                "active_task_count": 0,
                "throughput": {"completed_count": 0, "failed_count": 0, "skipped_count": 0},
            },
        }
    return json.loads(path.read_text(encoding="utf-8"))
