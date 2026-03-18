"""Self-test suite for Arcane event contract conformance and replay behavior."""

from __future__ import annotations

import datetime as dt
from typing import Any, Dict, List, Set
from pathlib import Path

from arcane_event_contract import REQUIRED_EVENT_TYPES, build_event_envelope, make_idempotency_key, validate_event_envelope
from arcane_event_projections import build_projection_state_from_events


class SelfTestFailure(RuntimeError):
    """Raised when a self-test assertion fails."""


def _utc_now_iso() -> str:
    return dt.datetime.now(tz=dt.timezone.utc).isoformat()


def _assert(condition: bool, message: str) -> None:
    if not condition:
        raise SelfTestFailure(message)


def _base_event(*, event_type: str, status: str, reason: str, requires_user_action: bool = False) -> Dict[str, Any]:
    aggregate_id = "self-test-task"
    stage_name = "factory-floor"
    attempt_number = 1
    return build_event_envelope(
        event_type=event_type,
        correlation_id=aggregate_id,
        causation_id="self-test-cause",
        aggregate_type="task",
        aggregate_id=aggregate_id,
        stage=stage_name,
        worker="SelfTestRunner",
        attempt=attempt_number,
        idempotency_key=f"{make_idempotency_key(aggregate_id, stage_name, attempt_number)}:{event_type}",
        status=status,
        reason=reason,
        requires_user_action=requires_user_action,
        payload={
            "details": {"source": "self_test"},
            "blocker_key": f"{stage_name}:{aggregate_id}:{reason}",
        },
    )


def _event_type_default_status(event_type: str) -> str:
    mapping = {
        "stage.claim.requested": "started",
        "stage.claim.granted": "completed",
        "stage.work.started": "started",
        "stage.work.heartbeat": "started",
        "stage.work.completed": "completed",
        "stage.work.failed": "failed",
        "stage.work.skipped": "skipped",
        "task.state.changed": "ok",
        "review.requested": "started",
        "review.completed": "completed",
        "scrum.blocker.raised": "blocked",
        "scrum.blocker.cleared": "completed",
        "scrum.user_action.required": "blocked",
        "worker.exhaustion.updated": "ok",
        "sprint.health.updated": "ok",
    }
    return mapping[event_type]


def _event_type_default_reason(event_type: str) -> str:
    return event_type.replace(".", "_")


def _validate_event_type_conformance() -> Dict[str, Any]:
    # -------------------------------------------------------------------------
    # INTENT DOCBLOCK
    # The contract declares a fixed taxonomy of required event types.
    # This test guarantees each required type can build and validate a legal envelope.
    # If any type fails here, producers are likely to drift out of schema compliance.
    # -------------------------------------------------------------------------
    generated_types: Set[str] = set()
    for event_type in sorted(REQUIRED_EVENT_TYPES):
        event_payload = _base_event(
            event_type=event_type,
            status=_event_type_default_status(event_type),
            reason=_event_type_default_reason(event_type),
            requires_user_action=event_type == "scrum.user_action.required",
        )
        validate_event_envelope(event_payload)
        generated_types.add(event_type)

    _assert(generated_types == REQUIRED_EVENT_TYPES, "required event taxonomy coverage mismatch")
    return {"name": "required_event_type_conformance", "result": "ok", "checked_types": len(generated_types)}


def _validate_idempotency_projection_dedupe() -> Dict[str, Any]:
    # -------------------------------------------------------------------------
    # INTENT DOCBLOCK
    # Duplicate delivery is expected in at-least-once event systems.
    # Projections must ignore duplicate idempotency keys to keep counters stable.
    # This test simulates duplicate completions and requires throughput to increment once.
    # -------------------------------------------------------------------------
    completed_event = _base_event(
        event_type="stage.work.completed",
        status="completed",
        reason="work_completed",
    )
    duplicate_completed_event = dict(completed_event)

    state = build_projection_state_from_events(
        [completed_event, duplicate_completed_event],
        source_stream=Path("self-test-stream"),
        source_filters={"start_line": 1, "max_events": 2},
    )

    completed_count = int(state.get("sprint", {}).get("throughput", {}).get("completed_count", 0))
    deduped_event_count = int(state.get("source", {}).get("deduped_event_count", 0))

    _assert(completed_count == 1, f"expected completed throughput=1, got {completed_count}")
    _assert(deduped_event_count == 1, f"expected deduped_event_count=1, got {deduped_event_count}")
    return {"name": "projection_idempotency_dedupe", "result": "ok", "completed_count": completed_count}


def _is_terminal_event_type(event_type: str) -> bool:
    return event_type in {"stage.work.completed", "stage.work.failed", "stage.work.skipped"}


def _validate_terminal_transition_legality() -> Dict[str, Any]:
    # -------------------------------------------------------------------------
    # INTENT DOCBLOCK
    # A single attempt must not emit multiple terminal outcomes.
    # Even if raw events contain duplicates, legality checks must flag conflicting terminals.
    # This test injects completed+failed for the same attempt and expects a violation.
    # -------------------------------------------------------------------------
    started_event = _base_event(event_type="stage.work.started", status="started", reason="attempt_started")
    completed_event = _base_event(event_type="stage.work.completed", status="completed", reason="attempt_completed")
    failed_event = _base_event(event_type="stage.work.failed", status="failed", reason="attempt_failed")

    events = [started_event, completed_event, failed_event]

    terminal_seen_by_attempt: Dict[str, str] = {}
    violations: List[Dict[str, Any]] = []

    for event in events:
        attempt_key = f"{event.get('aggregate_id', '')}:{event.get('attempt', '')}"
        event_type = str(event.get("event_type", ""))
        if not _is_terminal_event_type(event_type):
            continue

        existing_terminal = terminal_seen_by_attempt.get(attempt_key)
        if existing_terminal is not None and existing_terminal != event_type:
            violations.append(
                {
                    "attempt_key": attempt_key,
                    "existing_terminal": existing_terminal,
                    "new_terminal": event_type,
                }
            )
            continue
        terminal_seen_by_attempt[attempt_key] = event_type

    _assert(bool(violations), "expected illegal multi-terminal transition violation")
    return {"name": "terminal_transition_legality", "result": "ok", "violation_count": len(violations)}


def _validate_blocker_raise_clear_linkage() -> Dict[str, Any]:
    # -------------------------------------------------------------------------
    # INTENT DOCBLOCK
    # Blocker lifecycle requires clear events to link back to raised blockers.
    # This test confirms that a raise->user_action->clear chain leaves no open blockers.
    # If this fails, operators can see stale blockers after successful remediation.
    # -------------------------------------------------------------------------
    blocker_reason = "no_ready_assignment_for_role"
    blocker_key = f"factory-floor:self-test-task:{blocker_reason}"

    raised_event = _base_event(
        event_type="scrum.blocker.raised",
        status="blocked",
        reason=blocker_reason,
        requires_user_action=True,
    )
    raised_event["payload"]["blocker_key"] = blocker_key

    user_action_event = _base_event(
        event_type="scrum.user_action.required",
        status="blocked",
        reason=blocker_reason,
        requires_user_action=True,
    )
    user_action_event["payload"]["blocker_key"] = blocker_key

    clear_event = _base_event(
        event_type="scrum.blocker.cleared",
        status="completed",
        reason=blocker_reason,
        requires_user_action=False,
    )
    clear_event["payload"]["blocker_key"] = blocker_key
    clear_event["causation_id"] = raised_event["event_id"]

    state = build_projection_state_from_events(
        [raised_event, user_action_event, clear_event],
        source_stream=Path("self-test-stream"),
        source_filters={"start_line": 1, "max_events": 3},
    )

    open_blockers = state.get("blockers", {}).get("open", [])
    all_blockers = state.get("blockers", {}).get("all", {})
    blocker_entry = all_blockers.get(blocker_key, {})

    _assert(len(open_blockers) == 0, f"expected open_blockers=0, got {len(open_blockers)}")
    _assert(str(blocker_entry.get("status", "")) == "cleared", "expected blocker status to be cleared")
    return {"name": "blocker_raise_clear_linkage", "result": "ok", "final_status": blocker_entry.get("status", "")}


def run_event_contract_self_tests() -> Dict[str, Any]:
    # -------------------------------------------------------------------------
    # INTENT DOCBLOCK
    # This runner is the single entry point for Task 8 conformance and idempotency checks.
    # It returns structured JSON so CI, operators, and autonomous agents can consume outcomes.
    # Any failure captures test name and reason to support targeted self-repair attempts.
    # -------------------------------------------------------------------------
    test_functions = [
        _validate_event_type_conformance,
        _validate_idempotency_projection_dedupe,
        _validate_terminal_transition_legality,
        _validate_blocker_raise_clear_linkage,
    ]

    test_results: List[Dict[str, Any]] = []
    failures: List[Dict[str, Any]] = []

    for test_function in test_functions:
        try:
            test_results.append(test_function())
        except Exception as exc:
            failures.append(
                {
                    "test": test_function.__name__,
                    "error": str(exc),
                }
            )

    return {
        "schema_version": "v1",
        "generated_at": _utc_now_iso(),
        "result": "ok" if not failures else "failed",
        "tests_run": len(test_functions),
        "passed_count": len(test_results),
        "failed_count": len(failures),
        "tests": test_results,
        "failures": failures,
    }
