#!/usr/bin/env python3
"""Queue-driven stage workers for ArcaneArcadeMachineFactory.

Unlike stage_runtime.py (single-pass orchestration), this script runs each stage
as an independent worker that consumes stage inbox jobs and enqueues downstream jobs.
"""

from __future__ import annotations

import argparse
import datetime as dt
import json
import os
import re
import shutil
import socket
import subprocess
import sys
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Tuple

from arcane_event_contract import EventSchemaValidationError, build_event_envelope, make_idempotency_key, validate_event_envelope
import arcane_event_contract_self_tests
from arcane_event_projections import (
    build_projection_state,
    build_projection_state_from_events,
    load_event_stream_events,
    load_projection_state,
    write_projection_state,
)
from arcane_event_publishers import EventPublishError, EventPublisher, build_event_publisher

SCHEMA_VERSION = "v1"
PACKAGE_JOB_FILE = "next_job.json"
LETTER_FILE = "letter.toml"
MANIFEST_FILE = "manifest.toml"
CLAIM_LOCK_FILE = ".claim.lock"
CLAIM_LOCK_STALE_SEC = 900
POSTAL_SIGNAL_EXCHANGE = "postal.signals"
POSTAL_SIGNAL_ROUTING_KEY = "signal.ready"
POSTAL_SIGNAL_EVENT = "package_ready"
STAGE_NAMES: Tuple[str, ...] = (
    "factory-hopper",
    "writers-room",
    "observatory",
    "sprint-planning-room",
    "prompt-engineering-lab",
    "blueprint-forge",
    "factory-floor",
)
STAGE_SUBDIRS: Tuple[str, ...] = (
    "bin",
    "docs",
    "agents",
    "inbox",
    "outbox",
    "archive",
    "failed",
    "debug",
    "chatroom",
)
STAGE_AGENTS: Dict[str, str] = {
    "factory-hopper": "HopperNormalizerAgent",
    "writers-room": "WritersRoomAgent",
    "observatory": "ObservatoryAgent",
    "sprint-planning-room": "SprintPlanningAgent",
    "prompt-engineering-lab": "PromptEngineeringAgent",
    "blueprint-forge": "BlueprintForgeAgent",
    "factory-floor": "FactoryFloorAgent",
}
NEXT_STAGE: Dict[str, Optional[str]] = {
    "factory-hopper": "writers-room",
    "writers-room": "observatory",
    "observatory": "sprint-planning-room",
    "sprint-planning-room": "prompt-engineering-lab",
    "prompt-engineering-lab": "blueprint-forge",
    "blueprint-forge": "factory-floor",
    "factory-floor": None,
}

ASSIGNMENT_ROLE_BUDGETS: Dict[str, Dict[str, Any]] = {
    "architect": {"max_behavior_changes": 0, "max_prompt_chars": 8000, "max_context_chars": 12000},
    "coder": {"max_behavior_changes": 1, "max_prompt_chars": 12000, "max_context_chars": 16000},
    "qa": {"max_behavior_changes": 0, "max_prompt_chars": 8000, "max_context_chars": 12000},
}
ASSIGNMENT_MAX_INPUT_ARTIFACTS = 2
ASSIGNMENT_RETRY_POLICY: Dict[str, Any] = {
    "max_attempts": 2,
    "first_failure": "retry_same_role_narrowed_scope",
    "second_failure": "escalate_to_architect",
    "terminal_route": "failed_lane_with_remediation",
}

BLOCKER_REASON_POLICY: Dict[str, Dict[str, str]] = {
    "one_or_more_assignments_skipped": {"severity": "warning", "category": "dispatch"},
    "no_ready_assignment_for_role": {"severity": "critical", "category": "dispatch"},
    "prompt_budget_exceeded": {"severity": "critical", "category": "policy"},
}

_EVENT_PUBLISHER_CACHE: Dict[str, EventPublisher] = {}
WORK_HEARTBEAT_INTERVAL_SEC = 20.0
ROLLOUT_EMIT_MODES = {"legacy_only", "shadow", "contract_only"}
ROLLOUT_PROJECTION_MODES = {"legacy", "shadow", "enforce"}


@dataclass
class WorkerPaths:
    factory_root: Path
    hopper_root: Path
    stages_root: Path
    source_inbox: Path
    shared_chatroom_audit: Path
    active_project_file: Path
    stage_status_file: Path


class HandoffValidationError(RuntimeError):
    def __init__(self, details: Dict[str, Any]) -> None:
        self.details = details
        super().__init__("handoff contract validation failed")


class QAGateValidationError(RuntimeError):
    def __init__(self, details: Dict[str, Any]) -> None:
        self.details = details
        super().__init__("contract-level qa validation failed")


class AssignmentPolicyValidationError(RuntimeError):
    def __init__(self, details: Dict[str, Any]) -> None:
        self.details = details
        super().__init__("micro-task assignment policy validation failed")


def utc_now() -> str:
    return dt.datetime.now(tz=dt.timezone.utc).isoformat()


def slugify(value: str) -> str:
    return re.sub(r"[^a-zA-Z0-9_.-]+", "-", value).strip("-").lower()


def load_json(path: Path) -> Dict[str, Any]:
    with path.open("r", encoding="utf-8") as handle:
        return json.load(handle)


def write_json(path: Path, payload: Dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8") as handle:
        json.dump(payload, handle, indent=2, ensure_ascii=True)
        handle.write("\n")


def resolve_stages_root(factory_root: Path) -> Path:
    return factory_root / "ChatRooms" / "stages"


def append_jsonl(path: Path, payload: Dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("a", encoding="utf-8") as handle:
        handle.write(json.dumps(payload, ensure_ascii=True) + "\n")


def ensure_stage_layout(paths: WorkerPaths) -> None:
    for stage in STAGE_NAMES:
        for subdir in STAGE_SUBDIRS:
            (paths.stages_root / stage / subdir).mkdir(parents=True, exist_ok=True)
        ensure_stage_agent_layout(paths, stage)


def ensure_file(path: Path, content: str) -> None:
    if path.exists():
        return
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(content, encoding="utf-8")


def ensure_stage_agent_layout(paths: WorkerPaths, stage: str) -> None:
    agent_name = STAGE_AGENTS[stage]
    stage_root = paths.stages_root / stage
    agent_dir = stage_root / "agents" / slugify(agent_name)

    ensure_file(
        stage_root / "docs" / "README.md",
        "\n".join(
            [
                f"# {stage}",
                "",
                f"Primary agent: {agent_name}",
                "",
                "This stage consumes one package folder from inbox and emits one package folder in outbox.",
                f"The package must include {PACKAGE_JOB_FILE}.",
                "",
            ]
        ),
    )
    ensure_file(
        stage_root / "bin" / "README.md",
        "Stage-specific scripts can be added here when needed.\n",
    )
    ensure_file(
        agent_dir / "README.md",
        "\n".join(
            [
                f"# {agent_name}",
                "",
                f"Stage: {stage}",
                "",
                "Custom logic hook:",
                "- Implement `transform(job, package_dir, out_dir)` in `agent.py`.",
                "- Return a list of relative artifact paths to append into artifact_refs.",
                "",
            ]
        ),
    )
    ensure_file(
        agent_dir / "agent.py",
        "\n".join(
            [
                "\"\"\"Custom stage agent hook for TheFactoryHopper.\"\"\"",
                "",
                "from __future__ import annotations",
                "",
                "from pathlib import Path",
                "from typing import Any, Dict, List",
                "",
                "",
                "def transform(job: Dict[str, Any], package_dir: Path, out_dir: Path) -> List[str]:",
                "    \"\"\"Return additional relative artifact refs after stage processing.\"\"\"",
                "    # Add stage-specific custom behavior here when needed.",
                "    return []",
                "",
            ]
        ),
    )


def stage_dir(paths: WorkerPaths, stage: str, kind: str, job_id: str) -> Path:
    p = paths.stages_root / stage / kind / job_id
    p.mkdir(parents=True, exist_ok=True)
    return p


def stage_event(
    stage: str,
    actor: str,
    event: str,
    summary: str,
    artifact_refs: Iterable[str],
    status: str,
    error_code: str = "",
) -> Dict[str, Any]:
    return {
        "ts": utc_now(),
        "stage": stage,
        "actor": actor,
        "event": event,
        "summary": summary,
        "artifact_refs": list(artifact_refs),
        "status": status,
        "error_code": error_code,
    }


def write_stage_chatroom(paths: WorkerPaths, stage: str, payload: Dict[str, Any]) -> None:
    # Emit mode controls whether legacy chatroom signaling is active during rollout.
    # In contract-only enforcement we intentionally suppress legacy writes.
    # Shadow mode keeps both channels active so parity can be verified safely.
    if _current_emit_mode(paths) == "contract_only":
        return
    stage_chat = paths.stages_root / stage / "chatroom" / "events.jsonl"
    append_jsonl(stage_chat, payload)
    append_jsonl(paths.shared_chatroom_audit, payload)


def _event_stream_path(paths: WorkerPaths) -> Path:
    # Allow explicit override so tests and shadow runs can write to isolated streams.
    # Default path remains inside TheFactoryHopper docs for easy inspection and replay.
    # Keeping path resolution here prevents transport code from depending on env parsing.
    override = os.environ.get("ARCANE_EVENT_STREAM_FILE", "").strip()
    if override:
        return Path(override).expanduser().resolve()
    return paths.hopper_root / "docs" / "event_stream" / "events.jsonl"


def _rollout_config_path(paths: WorkerPaths) -> Path:
    # Rollout config is persisted in docs so operators can inspect mode state.
    # One durable file keeps CLI commands and runtime emit gates in sync.
    # This file is intentionally simple JSON for shell-friendly automation.
    return paths.hopper_root / "docs" / "event_rollout" / "config.json"


def _default_rollout_config() -> Dict[str, Any]:
    # Shadow defaults preserve current behavior while enabling staged migration.
    # Projection shadow mode allows verification without forcing enforcement.
    # Phase label makes operator intent explicit in runbook and handoff docs.
    return {
        "schema_version": SCHEMA_VERSION,
        "emit_mode": "shadow",
        "projection_mode": "shadow",
        "phase": "shadow_emit",
        "updated_at": utc_now(),
    }


def _load_rollout_config(paths: WorkerPaths) -> Dict[str, Any]:
    # Ensure config file exists so runtime emits always have deterministic defaults.
    config_path = _rollout_config_path(paths)
    if not config_path.exists():
        config = _default_rollout_config()
        write_json(config_path, config)
        return config

    # Corrupt config is replaced by safe defaults to keep workers running.
    try:
        config = load_json(config_path)
    except Exception:
        config = _default_rollout_config()

    # Normalize mode values so stale/invalid entries cannot break emit gates.
    emit_mode = str(config.get("emit_mode", "shadow")).strip().lower()
    projection_mode = str(config.get("projection_mode", "shadow")).strip().lower()
    if emit_mode not in ROLLOUT_EMIT_MODES:
        emit_mode = "shadow"
    if projection_mode not in ROLLOUT_PROJECTION_MODES:
        projection_mode = "shadow"

    # Preserve compatibility with older config revisions by backfilling fields.
    config["schema_version"] = SCHEMA_VERSION
    config["emit_mode"] = emit_mode
    config["projection_mode"] = projection_mode
    config.setdefault("phase", "shadow_emit")
    config.setdefault("updated_at", utc_now())
    return config


def _save_rollout_config(paths: WorkerPaths, config: Dict[str, Any]) -> None:
    # Config writes are normalized so invalid values never persist across runs.
    # Updating timestamp on each save gives an auditable cutover timeline.
    # Validation here keeps parser handlers lightweight and predictable.
    emit_mode = str(config.get("emit_mode", "shadow")).strip().lower()
    projection_mode = str(config.get("projection_mode", "shadow")).strip().lower()
    if emit_mode not in ROLLOUT_EMIT_MODES:
        raise SystemExit(f"Invalid emit_mode: {emit_mode}. Expected one of {sorted(ROLLOUT_EMIT_MODES)}")
    if projection_mode not in ROLLOUT_PROJECTION_MODES:
        raise SystemExit(
            f"Invalid projection_mode: {projection_mode}. Expected one of {sorted(ROLLOUT_PROJECTION_MODES)}"
        )

    config["schema_version"] = SCHEMA_VERSION
    config["emit_mode"] = emit_mode
    config["projection_mode"] = projection_mode
    config["updated_at"] = utc_now()
    write_json(_rollout_config_path(paths), config)


def _current_emit_mode(paths: WorkerPaths) -> str:
    return str(_load_rollout_config(paths).get("emit_mode", "shadow"))


def _current_projection_mode(paths: WorkerPaths) -> str:
    return str(_load_rollout_config(paths).get("projection_mode", "shadow"))


def _load_jsonl_records(path: Path) -> List[Dict[str, Any]]:
    # JSONL helper ignores malformed lines so diagnostics remain resilient.
    # This is used by parity checks that should not fail on one bad row.
    # Returned list order matches file order for deterministic comparisons.
    if not path.exists():
        return []

    records: List[Dict[str, Any]] = []
    for line in path.read_text(encoding="utf-8").splitlines():
        if not line.strip():
            continue
        try:
            payload = json.loads(line)
        except Exception:
            continue
        if isinstance(payload, dict):
            records.append(payload)
    return records


def _legacy_projection_snapshot(paths: WorkerPaths) -> Dict[str, Any]:
    # Legacy snapshot provides a fallback view when projection mode is legacy.
    # Queue depth is calculated per stage so operators can spot backlog pressure.
    # Stage status details are passed through unchanged for continuity.
    status_doc = load_json(paths.stage_status_file) if paths.stage_status_file.exists() else {"stages": {}}
    stage_status = status_doc.get("stages", {}) if isinstance(status_doc, dict) else {}
    queue_by_stage = {stage: queue_depth(paths, stage) for stage in STAGE_NAMES}

    return {
        "schema_version": SCHEMA_VERSION,
        "updated_at": utc_now(),
        "projection_mode": "legacy",
        "queue_depth_by_stage": queue_by_stage,
        "stage_status": stage_status,
    }


def _extract_signal_reason(summary: str) -> str:
    # Legacy dispatch summaries encode reason text after a fixed prefix.
    # We normalize suffixes so parity compares semantic reason values only.
    # Empty summary falls back to unknown for deterministic counting.
    text = str(summary).strip()
    prefix = "Scrum-Master signal:"
    if text.startswith(prefix):
        text = text[len(prefix) :].strip()
    text = text.replace("(user action requested)", "").strip()
    return text or "unknown"


def _build_rollout_parity_report(paths: WorkerPaths) -> Dict[str, Any]:
    # Parity compares legacy Scrum-Master interaction events to canonical blocker events.
    # Reason-level counting reveals drift even when total counts appear similar.
    # This report is the verify-phase gate before contract-only enforcement.
    legacy_events = _load_jsonl_records(paths.shared_chatroom_audit)
    contract_events = load_event_stream_events(_event_stream_path(paths))

    # Filter legacy audit stream down to Scrum-Master dispatch interactions only.
    legacy_signal_events = [
        item
        for item in legacy_events
        if str(item.get("actor", "")) == "TessScrumMaster" and str(item.get("event", "")) == "dispatch_interaction"
    ]
    # Count by normalized reason to compare semantics instead of raw totals.
    legacy_counts: Dict[str, int] = {}
    for item in legacy_signal_events:
        reason = _extract_signal_reason(str(item.get("summary", "")))
        legacy_counts[reason] = legacy_counts.get(reason, 0) + 1

    # Exclude synthetic smoke events so parity reflects real operational signaling only.
    # Smoke events are useful for transport checks but should not gate rollout decisions.
    def _is_smoke_event(item: Dict[str, Any]) -> bool:
        payload = item.get("payload", {}) if isinstance(item.get("payload"), dict) else {}
        source = str(payload.get("source", "")).strip().lower()
        reason = str(item.get("reason", "")).strip().lower()
        return source == "contract-event-smoke" or "smoke" in reason

    # Canonical parity baseline uses blocker-raised events with optional user-action context.
    contract_blockers = [
        item
        for item in contract_events
        if str(item.get("event_type", "")) == "scrum.blocker.raised" and not _is_smoke_event(item)
    ]
    contract_user_action = [
        item
        for item in contract_events
        if str(item.get("event_type", "")) == "scrum.user_action.required" and not _is_smoke_event(item)
    ]
    contract_counts: Dict[str, int] = {}
    for item in contract_blockers:
        reason = str(item.get("reason", "")).strip() or "unknown"
        contract_counts[reason] = contract_counts.get(reason, 0) + 1

    # Produce reason-level deltas so verify phase can pinpoint mismatch causes quickly.
    all_reasons = sorted(set(legacy_counts.keys()) | set(contract_counts.keys()))
    by_reason: List[Dict[str, Any]] = []
    for reason in all_reasons:
        legacy_count = int(legacy_counts.get(reason, 0))
        contract_count = int(contract_counts.get(reason, 0))
        by_reason.append(
            {
                "reason": reason,
                "legacy_count": legacy_count,
                "contract_count": contract_count,
                "delta": contract_count - legacy_count,
                "parity": legacy_count == contract_count,
            }
        )

    parity_ok = all(item.get("parity", False) for item in by_reason)
    return {
        "schema_version": SCHEMA_VERSION,
        "generated_at": utc_now(),
        "legacy_source": str(paths.shared_chatroom_audit),
        "contract_source": str(_event_stream_path(paths)),
        "legacy_signal_count": len(legacy_signal_events),
        "contract_blocker_count": len(contract_blockers),
        "contract_user_action_count": len(contract_user_action),
        "reason_parity": by_reason,
        "parity_ok": parity_ok,
    }


def _resolve_event_publisher(paths: WorkerPaths) -> EventPublisher:
    # Publisher instances are cached per hopper root so repeated emits stay cheap.
    # The cache also ensures one process uses one configured transport consistently.
    # This is intentionally simple until Task 9 rollout modes need richer publisher management.
    cache_key = str(paths.hopper_root)
    cached = _EVENT_PUBLISHER_CACHE.get(cache_key)
    if cached is not None:
        return cached

    publisher_type = os.environ.get("ARCANE_EVENT_PUBLISHER", "file")
    publisher = build_event_publisher(
        publisher_type=publisher_type,
        event_stream_path=_event_stream_path(paths),
    )
    _EVENT_PUBLISHER_CACHE[cache_key] = publisher
    return publisher


def write_contract_event(paths: WorkerPaths, payload: Dict[str, Any]) -> None:
    # Contract events are append-only and durable so projection replay is deterministic.
    # Publisher abstraction keeps transport swap isolated from business logic paths.
    # Legacy chatroom events still run in parallel during migration.
    if _current_emit_mode(paths) == "legacy_only":
        return
    publisher = _resolve_event_publisher(paths)
    publisher.publish(payload)


def _package_claim_identity(package_dir: Path) -> Tuple[str, str]:
    # Claim events must be emitted before the package is fully loaded into worker flow.
    # Read the queued job payload when available so correlation stays stable end-to-end.
    # Fall back to package directory name so claim telemetry never blocks on bad payloads.
    job_id = package_dir.name
    try:
        payload = load_json(package_dir / PACKAGE_JOB_FILE)
        resolved_job_id = str(payload.get("job_id", "")).strip()
        if resolved_job_id:
            job_id = resolved_job_id
    except Exception:
        pass
    return job_id, job_id


def _emit_claim_requested(paths: WorkerPaths, stage: str, package_dir: Path, selection_order: int, selection_total: int) -> Dict[str, Any]:
    # Claim-requested events expose queue scanning decisions before lock acquisition succeeds.
    # This makes contention and skipped candidates visible without mutating stage state yet.
    # Selection metadata is included so operators can reconstruct deterministic ordering.
    aggregate_id, correlation_id = _package_claim_identity(package_dir)
    payload = build_event_envelope(
        event_type="stage.claim.requested",
        correlation_id=correlation_id,
        causation_id=f"claim-scan:{stage}",
        aggregate_type="task",
        aggregate_id=aggregate_id,
        stage=stage,
        worker="StageWorker",
        attempt=1,
        idempotency_key=f"{make_idempotency_key(aggregate_id, stage, 1)}:claim-requested:{selection_order}",
        status="started",
        reason="claim_attempt_started",
        payload={
            "package_dir": str(package_dir),
            "selection_order": selection_order,
            "selection_total": selection_total,
        },
    )
    write_contract_event(paths, payload)
    return payload


def _emit_claim_granted(
    paths: WorkerPaths,
    stage: str,
    package_dir: Path,
    claim_info: Dict[str, Any],
    requested_event_id: str,
) -> Dict[str, Any]:
    # Claim-granted events record the exact moment a worker wins the lock.
    # We chain causation back to the request event so replay can explain why claim succeeded.
    # Persisting claim_info here keeps lock recovery behavior auditable after archive moves.
    aggregate_id, correlation_id = _package_claim_identity(package_dir)
    payload = build_event_envelope(
        event_type="stage.claim.granted",
        correlation_id=correlation_id,
        causation_id=requested_event_id,
        aggregate_type="task",
        aggregate_id=aggregate_id,
        stage=stage,
        worker="StageWorker",
        attempt=1,
        idempotency_key=f"{make_idempotency_key(aggregate_id, stage, 1)}:claim-granted",
        status="completed",
        reason="claim_lock_acquired",
        payload={
            "package_dir": str(package_dir),
            "claim": claim_info,
        },
    )
    write_contract_event(paths, payload)
    return payload


def _work_attempt_context(stage: str, job: Dict[str, Any], claim_info: Dict[str, Any]) -> Dict[str, Any]:
    # Work-attempt context carries correlation and guard state across worker phases.
    # Keeping this in one dict avoids duplicate envelope assembly in each try/except branch.
    # Terminal guard state lives here so exactly one terminal event is emitted per attempt.
    job_id = str(job.get("job_id", "unknown"))
    attempt = int(job.get("attempt", 1) or 1)
    return {
        "stage": stage,
        "job_id": job_id,
        "attempt": attempt,
        "correlation_id": job_id,
        "worker": "StageWorker",
        "claim_granted_event_id": str(claim_info.get("claim_granted_event_id", f"claim:{stage}")),
        "last_event_id": "",
        "terminal_emitted": False,
        "last_heartbeat_monotonic": 0.0,
    }


def _emit_work_started(paths: WorkerPaths, ctx: Dict[str, Any]) -> Dict[str, Any]:
    # Start event is the execution boundary for one claimed attempt.
    # Causation ties work execution to the successful claim-granted signal.
    # This event id seeds heartbeat and terminal causation chaining.
    payload = build_event_envelope(
        event_type="stage.work.started",
        correlation_id=ctx["correlation_id"],
        causation_id=ctx["claim_granted_event_id"],
        aggregate_type="task",
        aggregate_id=ctx["job_id"],
        stage=ctx["stage"],
        worker=ctx["worker"],
        attempt=ctx["attempt"],
        idempotency_key=f"{make_idempotency_key(ctx['job_id'], ctx['stage'], ctx['attempt'])}:work-started",
        status="started",
        reason="work_attempt_started",
        payload={"phase": "start"},
    )
    write_contract_event(paths, payload)
    ctx["last_event_id"] = payload["event_id"]
    return payload


def _maybe_emit_work_heartbeat(paths: WorkerPaths, ctx: Dict[str, Any], phase: str, *, force: bool = False) -> Optional[Dict[str, Any]]:
    # Heartbeats are emitted at phase boundaries to provide liveness without deep stage rewrites.
    # Force mode is used right after work start so every long attempt has an early heartbeat.
    # Time-gated emission prevents noisy streams while preserving operator visibility.
    now = time.monotonic()
    last = float(ctx.get("last_heartbeat_monotonic", 0.0))
    if not force and (now - last) < WORK_HEARTBEAT_INTERVAL_SEC:
        return None

    payload = build_event_envelope(
        event_type="stage.work.heartbeat",
        correlation_id=ctx["correlation_id"],
        causation_id=str(ctx.get("last_event_id") or ctx["claim_granted_event_id"]),
        aggregate_type="task",
        aggregate_id=ctx["job_id"],
        stage=ctx["stage"],
        worker=ctx["worker"],
        attempt=ctx["attempt"],
        # Include phase and time bucket in the idempotency key to preserve monotonic heartbeat traces.
        # This keeps dedupe behavior stable while still allowing multiple heartbeats per attempt.
        idempotency_key=(
            f"{make_idempotency_key(ctx['job_id'], ctx['stage'], ctx['attempt'])}"
            f":work-heartbeat:{int(now)}:{slugify(phase)}"
        ),
        status="started",
        reason="work_attempt_heartbeat",
        payload={"phase": phase},
    )
    # Update context pointers so downstream emissions keep a strict causation chain.
    # This chain is replayed by projection tooling to explain runtime transitions.
    write_contract_event(paths, payload)
    ctx["last_event_id"] = payload["event_id"]
    ctx["last_heartbeat_monotonic"] = now
    return payload


def _emit_work_terminal(
    paths: WorkerPaths,
    ctx: Dict[str, Any],
    *,
    event_type: str,
    status: str,
    reason: str,
    artifact_refs: Optional[List[str]] = None,
    payload: Optional[Dict[str, Any]] = None,
) -> Optional[Dict[str, Any]]:
    # Terminal guard ensures one terminal event per attempt even across multiple exception branches.
    # Duplicate terminal emissions are skipped so projections remain deterministic and idempotent.
    # Causation links to the last heartbeat/start signal for replayable execution timelines.
    if bool(ctx.get("terminal_emitted", False)):
        return None

    terminal_payload = build_event_envelope(
        event_type=event_type,
        correlation_id=ctx["correlation_id"],
        causation_id=str(ctx.get("last_event_id") or ctx["claim_granted_event_id"]),
        aggregate_type="task",
        aggregate_id=ctx["job_id"],
        stage=ctx["stage"],
        worker=ctx["worker"],
        attempt=ctx["attempt"],
        # Terminal idempotency key is stable per attempt and event type.
        # This prevents duplicate terminal writes when failure branches converge.
        idempotency_key=f"{make_idempotency_key(ctx['job_id'], ctx['stage'], ctx['attempt'])}:{event_type}",
        status=status,
        reason=reason,
        artifact_refs=artifact_refs or [],
        payload=payload or {},
    )
    # Mark terminal emission immediately after append so repeat calls become no-op.
    # Single-terminal semantics are critical for projection correctness and replay fidelity.
    write_contract_event(paths, terminal_payload)
    ctx["last_event_id"] = terminal_payload["event_id"]
    ctx["terminal_emitted"] = True
    return terminal_payload


def queue_depth(paths: WorkerPaths, stage: str) -> int:
    # Queue depth must reflect package folders, because jobs are now directories.
    # We count only valid packages that contain the expected job payload file.
    # This keeps UI/operator metrics aligned with what workers can actually consume.
    inbox = paths.stages_root / stage / "inbox"
    package_count = len([p for p in inbox.iterdir() if p.is_dir() and (p / PACKAGE_JOB_FILE).exists()])

    # Backward compatibility: include legacy json jobs still present in old queues.
    # This avoids misleading zero-depth readings while older jobs are draining.
    legacy_count = len(list(inbox.glob("*.json")))
    return package_count + legacy_count


def update_stage_status(paths: WorkerPaths, stage: str, status: str, details: Dict[str, Any]) -> None:
    if paths.stage_status_file.exists():
        status_doc = load_json(paths.stage_status_file)
    else:
        status_doc = {"schema_version": SCHEMA_VERSION, "stages": {}}

    status_doc.setdefault("schema_version", SCHEMA_VERSION)
    status_doc.setdefault("updated_at", utc_now())
    status_doc.setdefault("stages", {})
    status_doc["updated_at"] = utc_now()
    status_doc["stages"][stage] = {
        "status": status,
        "updated_at": utc_now(),
        "details": details,
    }
    write_json(paths.stage_status_file, status_doc)


def maybe_send_ws_event(paths: WorkerPaths, payload: Dict[str, Any]) -> None:
    try:
        with socket.create_connection(("127.0.0.1", 8765), timeout=0.3):
            pass
    except OSError:
        return

    publish_script = paths.factory_root / "bridge" / "publish_event.py"
    if not publish_script.exists():
        return

    cmd = [
        sys.executable,
        str(publish_script),
        "--channel",
        "factory.chatroom",
        "--stage",
        str(payload.get("stage", "")),
        "--actor",
        str(payload.get("actor", "StageWorker")),
        "--event",
        str(payload.get("event", "stage_event")),
        "--summary",
        str(payload.get("summary", "")),
        "--status",
        str(payload.get("status", "ok")),
    ]

    for ref in payload.get("artifact_refs", []):
        cmd.extend(["--artifact-ref", str(ref)])

    try:
        subprocess.run(cmd, cwd=str(paths.factory_root), check=False, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
    except OSError:
        return


def read_active_project(active_path: Path) -> Dict[str, Any]:
    if not active_path.exists():
        return {"active_project": "", "updated_at": ""}
    return load_json(active_path)


def projects_state_path(paths: WorkerPaths) -> Path:
    return paths.active_project_file.parent / "projects_state.json"


def read_projects_state(paths: WorkerPaths) -> Dict[str, Any]:
    state_path = projects_state_path(paths)
    if not state_path.exists():
        return {"enabled_projects": [], "updated_at": ""}
    return load_json(state_path)


def allowed_projects(paths: WorkerPaths) -> List[Path]:
    projects = project_dirs(paths.source_inbox)
    enabled = set(read_projects_state(paths).get("enabled_projects", []))
    if not enabled:
        return projects
    return [p for p in projects if p.name in enabled]


def project_dirs(source_inbox: Path) -> List[Path]:
    if not source_inbox.exists():
        return []
    return sorted([p for p in source_inbox.iterdir() if p.is_dir()], key=lambda p: p.name)


def conversation_files(project_dir: Path) -> List[Path]:
    return sorted(project_dir.glob("*/conversation.json"), key=lambda p: p.stat().st_mtime, reverse=True)


def resolve_project_slug(paths: WorkerPaths, explicit_project: Optional[str]) -> str:
    projects = allowed_projects(paths)
    if explicit_project:
        if any(p.name == explicit_project for p in projects):
            return explicit_project
        raise SystemExit(f"Project not found or not enabled in inbox: {explicit_project}")

    active = read_active_project(paths.active_project_file).get("active_project", "")
    if active and any(p.name == active for p in projects):
        return active

    if not projects:
        raise SystemExit(f"No project folders found under {paths.source_inbox}")
    return projects[0].name


def pick_conversation(paths: WorkerPaths, project_slug: str, explicit_path: Optional[str]) -> Path:
    if explicit_path:
        candidate = Path(explicit_path).expanduser().resolve()
        if not candidate.exists():
            raise SystemExit(f"Conversation path not found: {candidate}")
        return candidate

    files = conversation_files(paths.source_inbox / project_slug)
    if not files:
        raise SystemExit(f"No conversation.json files found under {paths.source_inbox / project_slug}")
    return files[0]


def enqueue_job(paths: WorkerPaths, stage: str, job: Dict[str, Any]) -> Path:
    package_dir = paths.stages_root / stage / "inbox" / job["job_id"]
    package_dir.mkdir(parents=True, exist_ok=True)
    write_json(package_dir / PACKAGE_JOB_FILE, job)
    return package_dir


def _claim_lock_meta(stage: str) -> Dict[str, Any]:
    return {
        "ts": utc_now(),
        "pid": os.getpid(),
        "stage": stage,
    }


def _claim_package(stage: str, package_dir: Path) -> Optional[Dict[str, Any]]:
    lock_path = package_dir / CLAIM_LOCK_FILE
    reclaimed_stale_lock = False
    reclaimed_foreign_lock = False

    if lock_path.exists():
        lock_owner_stage = ""
        try:
            lock_owner_stage = json.loads(lock_path.read_text(encoding="utf-8")).get("stage", "")
        except Exception:
            lock_owner_stage = ""

        age_sec = time.time() - lock_path.stat().st_mtime
        if lock_owner_stage and lock_owner_stage != stage:
            try:
                lock_path.unlink()
                reclaimed_foreign_lock = True
            except OSError:
                return None
        elif age_sec > CLAIM_LOCK_STALE_SEC:
            try:
                lock_path.unlink()
                reclaimed_stale_lock = True
            except OSError:
                return None
        else:
            return None

    try:
        fd = os.open(str(lock_path), os.O_CREAT | os.O_EXCL | os.O_WRONLY)
        os.write(fd, json.dumps(_claim_lock_meta(stage), ensure_ascii=True).encode("utf-8"))
        os.close(fd)
    except OSError:
        return None

    return {
        "lock": "acquired",
        "lock_file": CLAIM_LOCK_FILE,
        "reclaimed_stale_lock": reclaimed_stale_lock,
        "reclaimed_foreign_lock": reclaimed_foreign_lock,
    }


def consume_one_job(paths: WorkerPaths, stage: str) -> Optional[Tuple[Path, Dict[str, Any]]]:
    inbox = paths.stages_root / stage / "inbox"
    packages = sorted(
        [p for p in inbox.iterdir() if p.is_dir() and (p / PACKAGE_JOB_FILE).exists()],
        key=lambda p: (p.stat().st_mtime_ns, p.name),
    )
    if packages:
        for index, package in enumerate(packages):
            requested_event = _emit_claim_requested(
                paths,
                stage,
                package,
                selection_order=index + 1,
                selection_total=len(packages),
            )
            claim = _claim_package(stage, package)
            if claim is None:
                continue
            claim["selection_order"] = index + 1
            claim["selection_total"] = len(packages)
            claim["claim_requested_event_id"] = requested_event["event_id"]
            granted_event = _emit_claim_granted(paths, stage, package, claim, requested_event["event_id"])
            claim["claim_granted_event_id"] = granted_event["event_id"]
            return package, claim
        return None

    # Backward-compatible fallback for legacy json queue files.
    files = sorted(inbox.glob("*.json"), key=lambda p: p.stat().st_mtime)
    if files:
        legacy = inbox / files[0].stem
        legacy.mkdir(parents=True, exist_ok=True)
        shutil.move(str(files[0]), str(legacy / PACKAGE_JOB_FILE))
        requested_event = _emit_claim_requested(paths, stage, legacy, selection_order=1, selection_total=1)
        claim = _claim_package(stage, legacy)
        if claim is None:
            return None
        claim["selection_order"] = 1
        claim["selection_total"] = 1
        claim["legacy_migrated"] = True
        claim["claim_requested_event_id"] = requested_event["event_id"]
        granted_event = _emit_claim_granted(paths, stage, legacy, claim, requested_event["event_id"])
        claim["claim_granted_event_id"] = granted_event["event_id"]
        return legacy, claim
    return None


def archive_job(paths: WorkerPaths, stage: str, package_dir: Path, state: str) -> None:
    target_dir = paths.stages_root / stage / state / package_dir.name
    # Re-runs may reuse the same job_id; keep prior archive snapshots intact.
    if target_dir.exists():
        suffix = dt.datetime.now(tz=dt.timezone.utc).strftime("%Y%m%d%H%M%S")
        target_dir = paths.stages_root / stage / state / f"{package_dir.name}--{suffix}"
    target_dir.mkdir(parents=True, exist_ok=True)
    for entry in package_dir.iterdir():
        shutil.move(str(entry), str(target_dir / entry.name))
    shutil.rmtree(package_dir, ignore_errors=True)


def _package_payload_files(package_dir: Path) -> List[Path]:
    files: List[Path] = []
    for path in package_dir.rglob("*"):
        if not path.is_file():
            continue
        rel = path.relative_to(package_dir)
        if rel.as_posix() in {PACKAGE_JOB_FILE, LETTER_FILE, MANIFEST_FILE, CLAIM_LOCK_FILE}:
            continue
        files.append(path)
    return files


def _write_manifest(package_dir: Path) -> None:
    payload_files = [p.relative_to(package_dir).as_posix() for p in _package_payload_files(package_dir)]
    manifest_lines = [f'package_path = "{package_dir}"', f'destination_hint = "{package_dir.parent.parent / "inbox" / package_dir.name}"', "", "files = ["]
    for rel in sorted(payload_files):
        manifest_lines.append(f'  "{rel}",')
    manifest_lines.append("]")
    (package_dir / MANIFEST_FILE).write_text("\n".join(manifest_lines) + "\n", encoding="utf-8")


def _write_letter(package_dir: Path, recipient: str) -> None:
    letter = f'recipient = "{recipient}"\npackage_id = "{package_dir.name}"\n'
    (package_dir / LETTER_FILE).write_text(letter, encoding="utf-8")


def _validate_handoff_package(stage: str, next_stage: str, package_dir: Path) -> None:
    required_files = [PACKAGE_JOB_FILE, LETTER_FILE, MANIFEST_FILE]
    missing_required_files = [name for name in required_files if not (package_dir / name).exists()]
    payload_files = _package_payload_files(package_dir)

    errors: Dict[str, Any] = {}
    if missing_required_files:
        errors["missing_required_files"] = missing_required_files
    if not payload_files:
        errors["payload_error"] = "no payload artifacts found in handoff package"

    if errors:
        raise HandoffValidationError(
            {
                "stage": stage,
                "next_stage": next_stage,
                "job_id": package_dir.name,
                "package_dir": str(package_dir),
                "errors": errors,
            }
        )


def _record_handoff_validation_failure(
    paths: WorkerPaths,
    stage: str,
    job: Dict[str, Any],
    error: HandoffValidationError,
) -> None:
    # Persist handoff validation failures next to stage artifacts for postmortem triage.
    job_id = job.get("job_id", "unknown-job")
    failed_dir = paths.stages_root / stage / "failed" / job_id
    failed_dir.mkdir(parents=True, exist_ok=True)

    write_json(
        failed_dir / "handoff_validation_error.json",
        {
            "ts": utc_now(),
            "error": str(error),
            "details": error.details,
        },
    )

    sender_package = paths.stages_root / stage / "outbox" / job_id
    if sender_package.exists():
        snapshot_dir = failed_dir / "handoff_package_snapshot"
        if snapshot_dir.exists():
            shutil.rmtree(snapshot_dir, ignore_errors=True)
        shutil.move(str(sender_package), str(snapshot_dir))


def _record_qa_gate_failure(
    paths: WorkerPaths,
    stage: str,
    job: Dict[str, Any],
    error: QAGateValidationError,
) -> None:
    # QA gate failures capture both machine-readable reason and package snapshot.
    job_id = job.get("job_id", "unknown-job")
    failed_dir = paths.stages_root / stage / "failed" / job_id
    failed_dir.mkdir(parents=True, exist_ok=True)

    write_json(
        failed_dir / "qa_validation_error.json",
        {
            "ts": utc_now(),
            "error": str(error),
            "details": error.details,
        },
    )

    sender_package = paths.stages_root / stage / "outbox" / job_id
    if sender_package.exists():
        snapshot_dir = failed_dir / "qa_gate_package_snapshot"
        if snapshot_dir.exists():
            shutil.rmtree(snapshot_dir, ignore_errors=True)
        shutil.move(str(sender_package), str(snapshot_dir))


def _record_assignment_policy_failure(
    paths: WorkerPaths,
    stage: str,
    job: Dict[str, Any],
    error: AssignmentPolicyValidationError,
) -> None:
    # Assignment policy failures are routed with retry/fallback context for recovery.
    job_id = job.get("job_id", "unknown-job")
    failed_dir = paths.stages_root / stage / "failed" / job_id
    failed_dir.mkdir(parents=True, exist_ok=True)

    write_json(
        failed_dir / "assignment_policy_error.json",
        {
            "ts": utc_now(),
            "error": str(error),
            "details": error.details,
        },
    )

    sender_package = paths.stages_root / stage / "outbox" / job_id
    if sender_package.exists():
        snapshot_dir = failed_dir / "assignment_policy_package_snapshot"
        if snapshot_dir.exists():
            shutil.rmtree(snapshot_dir, ignore_errors=True)
        shutil.move(str(sender_package), str(snapshot_dir))


def _enforce_assignment_policy(paths: WorkerPaths, stage: str, job: Dict[str, Any], refs: List[str]) -> None:
    # This policy gate runs only on factory-floor outputs where micro-task dispatch is defined.
    if stage != "factory-floor":
        return

    out_dir = paths.stages_root / stage / "outbox" / job["job_id"]
    sprint_dir = out_dir / "generated_sprint"
    # Source/target artifact paths are explicit so operators can inspect by hand.
    micro_path = sprint_dir / "effort-1-micro-tasks.json"
    policy_path = sprint_dir / "assignment_policy.json"
    plan_path = sprint_dir / "assignment_plan.json"

    errors: List[str] = []
    micro_tasks: List[Dict[str, Any]] = []

    if not micro_path.exists():
        errors.append("missing generated_sprint/effort-1-micro-tasks.json")
    else:
        # Normalize micro task payload so every downstream check is deterministic.
        payload = load_json(micro_path)
        items = payload.get("micro_tasks", []) if isinstance(payload, dict) else []
        if not isinstance(items, list):
            errors.append("micro_tasks must be a list")
            # Reset to empty list so downstream loops remain safe and predictable.
            items = []
        # Keep only dict records so malformed rows do not break policy generation.
        micro_tasks = [item for item in items if isinstance(item, dict)]

    # ---- policy snapshot: this is the auditable machine-readable policy contract ----
    policy_doc = {
        "schema_version": SCHEMA_VERSION,
        "ts": utc_now(),
        "stage": stage,
        "job_id": job["job_id"],
        "role_budgets": ASSIGNMENT_ROLE_BUDGETS,
        "limits": {
            "max_input_artifacts_per_task": ASSIGNMENT_MAX_INPUT_ARTIFACTS,
            "max_prompt_chars_by_role": {k: v["max_prompt_chars"] for k, v in ASSIGNMENT_ROLE_BUDGETS.items()},
            "max_context_chars_by_role": {k: v["max_context_chars"] for k, v in ASSIGNMENT_ROLE_BUDGETS.items()},
        },
        "retry_policy": ASSIGNMENT_RETRY_POLICY,
    }

    assignments: List[Dict[str, Any]] = []
    for item in micro_tasks:
        # ---- normalize each assignment candidate before applying role budgets ----
        item_id = str(item.get("id", "unknown"))
        role = str(item.get("owner_role", "")).strip()
        # Input artifact count is capped for low-capability model reliability.
        inputs = item.get("input_artifacts", [])
        if not isinstance(inputs, list):
            inputs = []

        if role not in ASSIGNMENT_ROLE_BUDGETS:
            errors.append(f"unassignable role for micro-task {item_id}: {role}")
            continue

        if len(inputs) > ASSIGNMENT_MAX_INPUT_ARTIFACTS:
            errors.append(
                f"input artifact budget exceeded for {item_id}: {len(inputs)} > {ASSIGNMENT_MAX_INPUT_ARTIFACTS}"
            )

        # Prompt budget approximates local model context pressure per assignment.
        prompt_chars = len(_safe_text(item.get("title"))) + len(_safe_text(item.get("done_when")))
        prompt_cap = int(ASSIGNMENT_ROLE_BUDGETS[role]["max_prompt_chars"])
        if prompt_chars > prompt_cap:
            errors.append(f"prompt budget exceeded for {item_id}: {prompt_chars} > {prompt_cap}")

        # ---- retry metadata is emitted with each assignment for deterministic recovery ----
        assignments.append(
            {
                "micro_task_id": item_id,
                "parent_task": _safe_text(item.get("parent_task"), ""),
                "owner_role": role,
                "dispatch_status": "ready",
                "budget": ASSIGNMENT_ROLE_BUDGETS[role],
                "retry_metadata": {
                    "attempt": 0,
                    "max_attempts": ASSIGNMENT_RETRY_POLICY["max_attempts"],
                    "fallback_role": "architect",
                    "route_on_failure": ASSIGNMENT_RETRY_POLICY["terminal_route"],
                },
            }
        )

    plan_doc = {
        "schema_version": SCHEMA_VERSION,
        "ts": utc_now(),
        "job_id": job["job_id"],
        "passed": not errors,
        "assignment_count": len(assignments),
        "assignments": assignments,
        "errors": errors,
    }

    # Persist both documents together so operators can diff policy vs realized plan.
    policy_path.write_text(json.dumps(policy_doc, indent=2, ensure_ascii=True) + "\n", encoding="utf-8")
    plan_path.write_text(json.dumps(plan_doc, indent=2, ensure_ascii=True) + "\n", encoding="utf-8")

    if "generated_sprint/assignment_policy.json" not in refs:
        refs.append("generated_sprint/assignment_policy.json")
    if "generated_sprint/assignment_plan.json" not in refs:
        refs.append("generated_sprint/assignment_plan.json")

    if errors:
        # Fail fast so invalid assignments never reach downstream role dispatch.
        raise AssignmentPolicyValidationError(
            {
                "stage": stage,
                "job_id": job["job_id"],
                "policy_path": str(policy_path),
                "plan_path": str(plan_path),
                "errors": errors,
                "retry_policy": ASSIGNMENT_RETRY_POLICY,
            }
        )


def _run_contract_qa_gate(paths: WorkerPaths, stage: str, job: Dict[str, Any], refs: List[str]) -> None:
    # Contract QA gate verifies closure invariants before the stage can be considered done.
    if stage != "factory-floor":
        return

    out_dir = paths.stages_root / stage / "outbox" / job["job_id"]
    sprint_dir = out_dir / "generated_sprint"
    micro_path = sprint_dir / "effort-1-micro-tasks.json"
    qa_report_path = sprint_dir / "qa_validation.json"

    errors: List[str] = []
    micro_tasks: List[Dict[str, Any]] = []

    if not micro_path.exists():
        errors.append("missing generated_sprint/effort-1-micro-tasks.json")
    else:
        # Parse with explicit fallback so report files always capture parser failures.
        try:
            payload = json.loads(micro_path.read_text(encoding="utf-8"))
        except Exception as exc:
            errors.append(f"invalid micro-task json: {exc}")
            # Continue with empty payload so QA report still gets written.
            payload = {}
        # Type check here keeps QA output deterministic even with malformed inputs.
        items = payload.get("micro_tasks", []) if isinstance(payload, dict) else []
        if not isinstance(items, list):
            errors.append("micro_tasks must be a list")
            items = []
        # Filter non-dict rows to avoid key-access failures in validation loop.
        micro_tasks = [item for item in items if isinstance(item, dict)]

    invalid_done_when_ids: List[str] = []
    invalid_effort_ids: List[str] = []
    invalid_owner_role_ids: List[str] = []
    valid_roles = {"architect", "coder", "qa"}

    for item in micro_tasks:
        # done_when/effort/owner_role are required to keep micro-task execution auditable.
        item_id = str(item.get("id", "unknown"))
        done_when = item.get("done_when")
        if not isinstance(done_when, str) or not done_when.strip():
            invalid_done_when_ids.append(item_id)
        # effort=1 is the contract for micro-task granularity.
        if item.get("effort") != 1:
            invalid_effort_ids.append(item_id)
        if item.get("owner_role") not in valid_roles:
            invalid_owner_role_ids.append(item_id)

    if invalid_done_when_ids:
        errors.append(f"missing done_when for ids: {', '.join(invalid_done_when_ids)}")
    if invalid_effort_ids:
        errors.append(f"invalid effort for ids: {', '.join(invalid_effort_ids)}")
    # owner_role gate prevents dispatch to undefined worker profiles.
    if invalid_owner_role_ids:
        errors.append(f"invalid owner_role for ids: {', '.join(invalid_owner_role_ids)}")

    # QA report is produced even on failure to preserve evidence for review/replay.
    qa_report = {
        "schema_version": SCHEMA_VERSION,
        "ts": utc_now(),
        "stage": stage,
        "gate": "contract-level-qa",
        "job_id": job["job_id"],
        "passed": not errors,
        "checks": {
            "micro_task_count": len(micro_tasks),
            "invalid_done_when_ids": invalid_done_when_ids,
            "invalid_effort_ids": invalid_effort_ids,
            "invalid_owner_role_ids": invalid_owner_role_ids,
        },
        "errors": errors,
    }

    qa_report_path.write_text(json.dumps(qa_report, indent=2, ensure_ascii=True) + "\n", encoding="utf-8")
    if "generated_sprint/qa_validation.json" not in refs:
        refs.append("generated_sprint/qa_validation.json")

    if errors:
        # Promotion is blocked when QA closure criteria fail.
        raise QAGateValidationError(
            {
                "stage": stage,
                "job_id": job["job_id"],
                "report_path": str(qa_report_path),
                "errors": errors,
            }
        )


def _signal_postal_service(sender: str, package_id: str) -> Tuple[bool, str]:
    try:
        import importlib

        pika = importlib.import_module("pika")
    except Exception as exc:
        return False, f"pika import failed: {exc}"

    rabbit_host = os.getenv("RABBITMQ_HOST", "localhost")
    rabbit_port = int(os.getenv("RABBITMQ_PORT", "5672"))
    rabbit_user = os.getenv("RABBITMQ_USER", "postalWorker")
    rabbit_pass = os.getenv("RABBITMQ_PASS", "D0n74G37Me")

    try:
        connection = pika.BlockingConnection(
            pika.ConnectionParameters(
                host=rabbit_host,
                port=rabbit_port,
                credentials=pika.PlainCredentials(rabbit_user, rabbit_pass),
            )
        )
        channel = connection.channel()
        channel.exchange_declare(exchange=POSTAL_SIGNAL_EXCHANGE, exchange_type="topic", durable=True)
        channel.basic_publish(
            exchange=POSTAL_SIGNAL_EXCHANGE,
            routing_key=POSTAL_SIGNAL_ROUTING_KEY,
            body=json.dumps(
                {
                    "event": POSTAL_SIGNAL_EVENT,
                    "sender": sender,
                    "package_id": package_id,
                }
            ),
        )
        connection.close()
        return True, "ok"
    except Exception as exc:
        return False, str(exc)


def _handoff_package(
    paths: WorkerPaths,
    stage: str,
    next_stage: str,
    job: Dict[str, Any],
    refs: List[str],
) -> Dict[str, Any]:
    sender_package = paths.stages_root / stage / "outbox" / job["job_id"]
    recipient_package = paths.stages_root / next_stage / "inbox" / job["job_id"]

    next_payload = _next_job(job, refs)
    next_payload.pop("package_dir", None)
    write_json(sender_package / PACKAGE_JOB_FILE, next_payload)
    _write_letter(sender_package, next_stage)
    _write_manifest(sender_package)
    _validate_handoff_package(stage, next_stage, sender_package)

    signal_ok, signal_message = _signal_postal_service(stage, job["job_id"])

    if signal_ok:
        for _ in range(10):
            if recipient_package.exists():
                return {"transport": "postal", "signal": "ok"}
            time.sleep(0.1)

    # Deterministic fallback when postal worker is unavailable.
    recipient_package.parent.mkdir(parents=True, exist_ok=True)
    if recipient_package.exists():
        shutil.rmtree(recipient_package)
    shutil.move(str(sender_package), str(recipient_package))
    return {"transport": "direct_fallback", "signal": signal_message if not signal_ok else "timeout"}


def _mark_success(
    paths: WorkerPaths,
    stage: str,
    job: Dict[str, Any],
    refs: List[str],
    claim_info: Optional[Dict[str, Any]] = None,
) -> None:
    event = stage_event(
        stage=stage,
        actor="StageWorker",
        event="artifact_transformed",
        summary=f"{stage} completed for {job['project_slug']}/{job['conversation_id']}",
        artifact_refs=refs,
        status="ok",
    )
    write_stage_chatroom(paths, stage, event)
    update_stage_status(
        paths,
        stage,
        "ok",
        {
            "project_slug": job["project_slug"],
            "conversation_id": job["conversation_id"],
            "artifact_count": len(refs),
            "queue_depth": queue_depth(paths, stage),
            "last_job_id": job["job_id"],
            "claim": claim_info or {},
        },
    )


def _mark_failure(paths: WorkerPaths, stage: str, job: Dict[str, Any], error: str) -> None:
    event = stage_event(
        stage=stage,
        actor="StageWorker",
        event="artifact_failed",
        summary=f"{stage} failed for {job.get('project_slug', 'unknown')}/{job.get('conversation_id', 'unknown')}",
        artifact_refs=[],
        status="failed",
        error_code="stage_failure",
    )
    write_stage_chatroom(paths, stage, event)
    update_stage_status(
        paths,
        stage,
        "failed",
        {
            "project_slug": job.get("project_slug", "unknown"),
            "conversation_id": job.get("conversation_id", "unknown"),
            "queue_depth": queue_depth(paths, stage),
            "last_error": error,
        },
    )


def _next_job(job: Dict[str, Any], refs: List[str]) -> Dict[str, Any]:
    cloned = dict(job)
    cloned["artifact_refs"] = refs
    cloned["updated_at"] = utc_now()
    return cloned


def _safe_text(value: Any, fallback: str = "") -> str:
    return str(value) if value is not None else fallback


def _extract_conversation_parts(source_doc: Dict[str, Any], limit: int = 16) -> List[str]:
    mapping = source_doc.get("mapping", {})
    parts: List[str] = []

    if isinstance(mapping, dict):
        for node in mapping.values():
            if not isinstance(node, dict):
                continue
            message = node.get("message", {})
            if not isinstance(message, dict):
                continue
            content = message.get("content", {})
            if not isinstance(content, dict):
                continue
            fragments = content.get("parts", [])
            if not isinstance(fragments, list):
                continue
            for frag in fragments:
                text = _safe_text(frag).strip()
                if text:
                    parts.append(text)
                if len(parts) >= limit:
                    return parts
    return parts


def _conversation_to_markdown(source_doc: Dict[str, Any], project_slug: str, conversation_id: str) -> str:
    title = _safe_text(source_doc.get("title"), "Untitled Conversation")
    parts = _extract_conversation_parts(source_doc)
    lines: List[str] = [f"# {title}", "", f"Project: {project_slug}", f"Conversation: {conversation_id}", ""]
    lines.append("## Extracted Conversation Snippets")
    if parts:
        for idx, text in enumerate(parts, start=1):
            lines.append(f"{idx}. {text}")
    else:
        lines.append("1. No extractable content found in conversation mapping.")
    lines.append("")
    return "\n".join(lines)


def _find_artifact_path(job: Dict[str, Any], filename: str) -> Path:
    package_dir = Path(job.get("package_dir", "")) if job.get("package_dir") else None
    for ref in job.get("artifact_refs", []):
        p = Path(ref)
        if p.name == filename:
            if package_dir is not None:
                candidate = package_dir / p
                if candidate.exists():
                    return candidate
            return p
    if package_dir is not None:
        fallback = package_dir / filename
        if fallback.exists():
            return fallback
    raise RuntimeError(f"required artifact not found: {filename}")


def _copy_payload_forward(job: Dict[str, Any], out_dir: Path) -> None:
    package_dir_value = job.get("package_dir")
    if not package_dir_value:
        return

    src = Path(package_dir_value)
    if not src.exists() or src == out_dir:
        return

    for path in src.rglob("*"):
        if not path.is_file():
            continue
        rel = path.relative_to(src)
        if rel.as_posix() in {PACKAGE_JOB_FILE, LETTER_FILE, MANIFEST_FILE, CLAIM_LOCK_FILE}:
            continue
        dest = out_dir / rel
        dest.parent.mkdir(parents=True, exist_ok=True)
        shutil.copy2(path, dest)


def _run_custom_agent_hook(paths: WorkerPaths, stage: str, job: Dict[str, Any], out_dir: Path) -> List[str]:
    agent_name = STAGE_AGENTS[stage]
    hook_file = paths.stages_root / stage / "agents" / slugify(agent_name) / "agent.py"
    if not hook_file.exists():
        return []

    cmd = [
        sys.executable,
        "-c",
        (
            "import importlib.util, json, pathlib, sys;"
            "hook=pathlib.Path(sys.argv[1]);"
            "job=json.loads(sys.argv[2]);"
            "package_dir=pathlib.Path(sys.argv[3]);"
            "out_dir=pathlib.Path(sys.argv[4]);"
            "spec=importlib.util.spec_from_file_location('stage_agent_hook', hook);"
            "mod=importlib.util.module_from_spec(spec);"
            "spec.loader.exec_module(mod);"
            "result=mod.transform(job, package_dir, out_dir) if hasattr(mod, 'transform') else [];"
            "print(json.dumps(result if isinstance(result, list) else []))"
        ),
        str(hook_file),
        json.dumps(job, ensure_ascii=True),
        str(Path(job.get("package_dir", out_dir))),
        str(out_dir),
    ]

    try:
        proc = subprocess.run(cmd, check=True, capture_output=True, text=True)
        parsed = json.loads(proc.stdout.strip() or "[]")
    except Exception:
        return []

    refs: List[str] = []
    for item in parsed:
        if isinstance(item, str) and item.strip():
            refs.append(item)
    return refs


def process_factory_hopper(paths: WorkerPaths, job: Dict[str, Any]) -> List[str]:
    stage = "factory-hopper"
    source_conv = Path(job["conversation_path"])
    source_doc = load_json(source_conv)

    out_dir = stage_dir(paths, stage, "outbox", job["job_id"])
    archive_dir = stage_dir(paths, stage, "archive", job["job_id"])

    conversation_md = _conversation_to_markdown(source_doc, job["project_slug"], job["conversation_id"])
    metadata_yaml = "\n".join(
        [
            "source: chatgpt",
            f"date: {utc_now()[0:10]}",
            f"topic: {slugify(_safe_text(source_doc.get('title'), 'factory-topic')).replace('-', ' ')}",
            "author: craig",
            f"project_slug: {job['project_slug']}",
            f"conversation_id: {job['conversation_id']}",
            "",
        ]
    )

    conversation_path = out_dir / "conversation.md"
    metadata_path = out_dir / "metadata.yaml"
    conversation_path.write_text(conversation_md, encoding="utf-8")
    metadata_path.write_text(metadata_yaml, encoding="utf-8")

    shutil.copy2(source_conv, archive_dir / "conversation.json")
    return ["conversation.md", "metadata.yaml"]


def process_writers(paths: WorkerPaths, job: Dict[str, Any]) -> List[str]:
    stage = "writers-room"
    out_dir = stage_dir(paths, stage, "outbox", job["job_id"])
    _copy_payload_forward(job, out_dir)

    conversation_path = _find_artifact_path(job, "conversation.md")
    conversation_text = conversation_path.read_text(encoding="utf-8")
    title_line = conversation_text.splitlines()[0] if conversation_text else "# Untitled"
    title = title_line.lstrip("# ").strip() if title_line.startswith("#") else "Untitled"

    summary = (
        "Project Summary\n\n"
        "Goal:\n"
        "Build a modular AI software factory with stage-based artifact flow.\n\n"
        "Key Concepts:\n"
        "- Artifact pipelines\n"
        "- Stage workers\n"
        "- Prompt engineering preparation\n"
        "- Observable status snapshots\n\n"
        "Audience:\n"
        "Developers building autonomous delivery pipelines\n"
    )
    vision = (
        "# Vision\n\n"
        f"Build {title} into a deterministic software design compiler from conversation to blueprint artifacts.\n"
    )
    personas = (
        "# Personas\n\n"
        "## Craig the Developer\n"
        "Goals:\n"
        "1. Build agent-driven software systems.\n"
        "2. Preserve observability across stage transitions.\n"
        "Pain Points:\n"
        "1. Context loss between sessions.\n"
        "2. Weak handoff from idea to implementation.\n"
    )
    goals = (
        "# Goals\n\n"
        "1. Convert conversations into implementation-ready planning artifacts.\n"
        "2. Preserve deterministic stage contracts.\n"
        "3. Improve coding prompt quality before implementation starts.\n"
    )
    scope = (
        "# Scope\n\n"
        "In Scope:\n"
        "1. Pre-code stages from hopper to blueprint forge.\n"
        "2. Artifact generation and queue-driven worker execution.\n\n"
        "Out of Scope:\n"
        "1. Full production deployment automation.\n"
    )
    assumptions = (
        "# Assumptions\n\n"
        "1. Conversation source remains JSON in inbox.\n"
        "2. Stage outputs are file artifacts under stage outbox paths.\n"
        "3. Prompt consumers use markdown artifacts directly.\n"
    )
    risks = (
        "# Risks\n\n"
        "1. Ambiguous requirements survive into planning stage.\n"
        "Mitigation: explicit assumptions and constraints artifacts.\n\n"
        "2. Prompt drift between architecture and tasks.\n"
        "Mitigation: prompt engineering stage requires tasks + constraints + features inputs.\n"
    )
    stories = (
        "# Stories\n\n"
        "Story: As a developer\n"
        "I want a conversation-to-vision pipeline\n"
        "So that ideas become structured project artifacts\n\n"
        "Story: As a coding agent\n"
        "I want architecture and constraints before coding\n"
        "So that implementation is deterministic and testable\n"
    )
    sprint = (
        "# Sprint Proposal\n\n"
        "Sprint Goal:\n"
        "Create the first 4 pre-code stages and verify artifact flow.\n\n"
        "Tasks:\n"
        "1. Build Factory Hopper outputs.\n"
        "2. Build Writers Room artifacts.\n"
        "3. Build Observatory research artifacts.\n"
        "4. Build Sprint Planning engineering-intent artifacts.\n"
    )

    artifacts = {
        "summary.md": summary,
        "vision.md": vision,
        "personas.md": personas,
        "goals.md": goals,
        "scope.md": scope,
        "assumptions.md": assumptions,
        "risks.md": risks,
        "stories.md": stories,
        "sprint.md": sprint,
    }

    refs: List[str] = []
    for filename, content in artifacts.items():
        target = out_dir / filename
        target.write_text(content, encoding="utf-8")
        refs.append(filename)

    return refs


def process_observatory(paths: WorkerPaths, job: Dict[str, Any]) -> List[str]:
    stage = "observatory"
    out_dir = stage_dir(paths, stage, "outbox", job["job_id"])
    _copy_payload_forward(job, out_dir)

    research = (
        "# Research\n\n"
        "1. File-based stage queues are simple, observable, and robust for MVP pipelines.\n"
        "2. Prompt quality improves when tasks and constraints are explicit before coding.\n"
        "3. Stage-local chatroom events improve troubleshooting and replay.\n"
    )
    patterns = (
        "# Patterns\n\n"
        "1. Artifact -> Stage -> Artifact\n"
        "Fit: deterministic handoff between agents.\n"
        "2. Prompt Compiler Stage\n"
        "Fit: converts planning artifacts into role-specific prompts.\n"
    )
    references = (
        "# References\n\n"
        "1. Gherkin feature specifications for behavior-first planning.\n"
        "2. Systemd + filesystem queue patterns for worker loops.\n"
        "3. WebSocket event channels for live UI status.\n"
    )
    similar_projects = (
        "# Similar Projects\n\n"
        "1. CI design pipelines that separate planning and implementation.\n"
        "2. Agentic backlog systems with claim-based execution.\n"
        "3. Artifact-first orchestration workflows in build systems.\n"
    )

    artifacts = {
        "research.md": research,
        "patterns.md": patterns,
        "references.md": references,
        "similar_projects.md": similar_projects,
    }

    refs: List[str] = []
    for filename, content in artifacts.items():
        target = out_dir / filename
        target.write_text(content, encoding="utf-8")
        refs.append(filename)
    return refs


def process_sprint_planning(paths: WorkerPaths, job: Dict[str, Any]) -> List[str]:
    stage = "sprint-planning-room"
    out_dir = stage_dir(paths, stage, "outbox", job["job_id"])
    _copy_payload_forward(job, out_dir)

    features = (
        "Feature: Writers Room Stage\n\n"
        "Scenario: Convert conversation to vision\n"
        "Given a conversation artifact\n"
        "When the writers room processes it\n"
        "Then a vision document should be generated\n\n"
        "Feature: Prompt Engineering Stage\n\n"
        "Scenario: Generate coding prompts from planning docs\n"
        "Given features and tasks artifacts\n"
        "When prompt engineering lab runs\n"
        "Then coder, qa, and architect prompts should be emitted\n"
    )
    architecture = (
        "# Architecture Proposal\n\n"
        "Pipeline Model:\n"
        "artifact -> stage -> artifact\n\n"
        "Workers:\n"
        "1. Python queue worker runtime for stage orchestration.\n"
        "2. Future Rust worker runtime for hardened daemon mode.\n\n"
        "Queue:\n"
        "filesystem + systemd\n"
    )
    stack = (
        "# Technology Stack\n\n"
        "1. Agent runtime: Python (current), Rust (target).\n"
        "2. AI integration: Python LLM connectors.\n"
        "3. Orchestration: systemd + filesystem queues.\n"
        "4. Messaging: local websocket broker for live status.\n"
    )
    constraints = (
        "# Constraints\n\n"
        "1. Preserve append-only chatroom events.\n"
        "2. Preserve schema_version v1 for current artifacts.\n"
        "3. Keep stage outputs deterministic and file-based.\n"
        "4. Enforce enabled-project allowlist in workers.\n"
    )
    tasks = (
        "# Tasks\n\n"
        "Task 1\n"
        "Create writer agent skeleton\n\n"
        "Task 2\n"
        "Create stage worker templates for observatory and planning\n\n"
        "Task 3\n"
        "Create prompt templates for coder, qa, and architect roles\n"
    )

    artifacts = {
        "features.feature": features,
        "architecture.md": architecture,
        "stack.md": stack,
        "constraints.md": constraints,
        "tasks.md": tasks,
    }

    refs: List[str] = []
    for filename, content in artifacts.items():
        target = out_dir / filename
        target.write_text(content, encoding="utf-8")
        refs.append(filename)
    return refs


def process_prompt_engineering(paths: WorkerPaths, job: Dict[str, Any]) -> List[str]:
    stage = "prompt-engineering-lab"
    out_dir = stage_dir(paths, stage, "outbox", job["job_id"])
    _copy_payload_forward(job, out_dir)
    prompt_dir = out_dir / "agent_prompts"
    prompt_dir.mkdir(parents=True, exist_ok=True)

    coder_prompt = (
        "You are Mason, the Developer Agent.\n\n"
        "Context:\n"
        "Rust worker runtime target with Python brain and filesystem queue model.\n\n"
        "Goal:\n"
        "Implement queue claiming and deterministic artifact handoff modules.\n\n"
        "Constraints:\n"
        "1. Filesystem job queue\n"
        "2. systemd compatibility\n"
        "3. append-only event logs\n\n"
        "Expected Output:\n"
        "1. Queue module implementation\n"
        "2. Artifact schema-compliant outputs\n"
    )
    qa_prompt = (
        "You are Rowan, the QA Agent.\n\n"
        "Context:\n"
        "Validate against features.feature and constraints.md.\n\n"
        "Goal:\n"
        "Find behavioral regressions and contract violations.\n\n"
        "Expected Output:\n"
        "1. Validation report by scenario\n"
        "2. Defect list with severity and repro steps\n"
    )
    architect_prompt = (
        "You are Seraphine, the Architect Agent.\n\n"
        "Context:\n"
        "Pre-code stages have generated architecture, stack, and tasks.\n\n"
        "Goal:\n"
        "Refine module boundaries and hard constraints before implementation.\n\n"
        "Expected Output:\n"
        "1. Architecture decision notes\n"
        "2. Risk and mitigation list\n"
    )

    artifacts = {
        "coder_prompt.md": coder_prompt,
        "qa_prompt.md": qa_prompt,
        "architect_prompt.md": architect_prompt,
    }

    refs: List[str] = []
    for filename, content in artifacts.items():
        target = prompt_dir / filename
        target.write_text(content, encoding="utf-8")
        refs.append(f"agent_prompts/{filename}")
    return refs


def process_blueprint_forge(paths: WorkerPaths, job: Dict[str, Any]) -> List[str]:
    stage = "blueprint-forge"
    out_dir = stage_dir(paths, stage, "outbox", job["job_id"])
    _copy_payload_forward(job, out_dir)
    blueprint_dir = out_dir / "repo_blueprint"
    blueprint_dir.mkdir(parents=True, exist_ok=True)

    directory_structure = (
        "# Directory Structure\n\n"
        "1. worker/\n"
        "2. brain/\n"
        "3. systemd/\n"
        "4. stages/\n"
        "5. schemas/\n"
        "6. tests/\n"
        "7. docs/\n"
        "8. inbox/\n"
        "9. outbox/\n"
        "10. failed/\n"
        "11. archive/\n"
    )
    module_plan = (
        "# Module Plan\n\n"
        "1. queue_claim\n"
        "2. artifact_transform\n"
        "3. chatroom_logger\n"
        "4. status_snapshot\n"
        "5. bridge_publisher\n"
    )
    api_contracts = (
        "# API Contracts\n\n"
        "## Stage Event Contract v1\n"
        "event_id, ts, stage, actor, event, summary, artifact_refs, status, error_code, schema_version\n\n"
        "## Stage Status Contract v1\n"
        "schema_version, updated_at, stages\n"
    )

    artifacts = {
        "directory_structure.md": directory_structure,
        "module_plan.md": module_plan,
        "api_contracts.md": api_contracts,
    }

    refs: List[str] = []
    for filename, content in artifacts.items():
        target = blueprint_dir / filename
        target.write_text(content, encoding="utf-8")
        refs.append(f"repo_blueprint/{filename}")

    # Emit a final stage completion event to live consumers.
    final_event = {
        "schema_version": SCHEMA_VERSION,
        "event_id": f"{job['job_id']}--{int(dt.datetime.now().timestamp())}",
        "ts": utc_now(),
        "stage": stage,
        "actor": "BlueprintForgeAgent",
        "event": "blueprint_emitted",
        "summary": "Pre-code blueprint artifacts emitted",
        "artifact_refs": refs,
        "status": "ok",
        "error_code": "",
    }
    maybe_send_ws_event(paths, final_event)
    return refs


def _safe_read_text(path: Path) -> str:
    if not path.exists() or not path.is_file():
        return ""
    return path.read_text(encoding="utf-8")


def _extract_enumerated_items(text: str) -> List[str]:
    items: List[str] = []
    for raw in text.splitlines():
        line = raw.strip()
        if not line:
            continue
        if re.match(r"^\d+\.\s+", line):
            items.append(re.sub(r"^\d+\.\s+", "", line).strip())
            continue
        if line.startswith("- "):
            items.append(line[2:].strip())
    return items


def process_factory_floor(paths: WorkerPaths, job: Dict[str, Any]) -> List[str]:
    stage = "factory-floor"
    out_dir = stage_dir(paths, stage, "outbox", job["job_id"])
    _copy_payload_forward(job, out_dir)

    # Factory Floor compiles planning context into implementation-ready micro tasks.
    # The output format intentionally mirrors Sprint task docs (0..N) for continuity.
    # effort=1 slices are generated to keep low-capability LLM execution viable.
    tasks_text = _safe_read_text(out_dir / "tasks.md")
    features_text = _safe_read_text(out_dir / "features.feature")
    constraints_text = _safe_read_text(out_dir / "constraints.md")
    coder_prompt = _safe_read_text(out_dir / "agent_prompts" / "coder_prompt.md")
    qa_prompt = _safe_read_text(out_dir / "agent_prompts" / "qa_prompt.md")
    architect_prompt = _safe_read_text(out_dir / "agent_prompts" / "architect_prompt.md")

    tasks = _extract_enumerated_items(tasks_text)
    if not tasks:
        tasks = [
            "Build deterministic queue claim path",
            "Implement stage artifact handoff checks",
            "Create contract-level QA validation step",
        ]

    micro_tasks: List[Dict[str, Any]] = []
    for index, task in enumerate(tasks, start=1):
        base_id = f"T{index:02d}"
        micro_tasks.append(
            {
                "id": f"{base_id}-M1",
                "parent_task": base_id,
                "title": f"Define acceptance for: {task}",
                "effort": 1,
                "owner_role": "architect",
                "input_artifacts": ["constraints.md", "features.feature"],
                "done_when": "Acceptance criteria is explicit and testable.",
            }
        )
        micro_tasks.append(
            {
                "id": f"{base_id}-M2",
                "parent_task": base_id,
                "title": f"Implement smallest code slice for: {task}",
                "effort": 1,
                "owner_role": "coder",
                "input_artifacts": ["tasks.md", "coder_prompt.md"],
                "done_when": "One small behavior change is committed in local patch form.",
            }
        )
        micro_tasks.append(
            {
                "id": f"{base_id}-M3",
                "parent_task": base_id,
                "title": f"Validate behavior and regressions for: {task}",
                "effort": 1,
                "owner_role": "qa",
                "input_artifacts": ["features.feature", "qa_prompt.md"],
                "done_when": "Validation result is recorded with pass/fail and repro notes.",
            }
        )

    sprint_goal = (
        "# Sprint Goal\n\n"
        "Compile planning artifacts into execution-ready, effort-1 micro-tasks so low-capability LLM workers can deliver incrementally.\n"
    )
    context_bundle = {
        "job_id": job["job_id"],
        "project_slug": job["project_slug"],
        "conversation_id": job["conversation_id"],
        "tasks_preview": tasks,
        "has_features": bool(features_text.strip()),
        "has_constraints": bool(constraints_text.strip()),
        "has_prompts": bool(coder_prompt.strip() and qa_prompt.strip() and architect_prompt.strip()),
    }

    sprint_dir = out_dir / "generated_sprint"
    sprint_dir.mkdir(parents=True, exist_ok=True)

    (sprint_dir / "sprint_goal.md").write_text(sprint_goal, encoding="utf-8")
    (sprint_dir / "context_bundle.json").write_text(json.dumps(context_bundle, indent=2, ensure_ascii=True) + "\n", encoding="utf-8")

    for idx, task in enumerate(tasks, start=0):
        task_doc = "\n".join(
            [
                f"# Task {idx}: {task}",
                "",
                "## Goals",
                "",
                f"- deliver a minimal, testable slice for: {task}",
                "",
                "## Requirements",
                "",
                "- one narrow behavior change",
                "- one verification step",
                "- no scope expansion",
                "",
                "## Acceptance Criteria",
                "",
                "- change is observable",
                "- regressions checked",
                "",
                "## Implementation Steps",
                "",
                "1. Specify smallest acceptance.",
                "2. Implement minimal patch.",
                "3. Validate and record result.",
            ]
        )
        (sprint_dir / f"{idx}-tasks.md").write_text(task_doc + "\n", encoding="utf-8")

    (sprint_dir / "effort-1-micro-tasks.json").write_text(
        json.dumps({"schema_version": SCHEMA_VERSION, "micro_tasks": micro_tasks}, indent=2, ensure_ascii=True) + "\n",
        encoding="utf-8",
    )

    refs = [
        "generated_sprint/sprint_goal.md",
        "generated_sprint/context_bundle.json",
        "generated_sprint/effort-1-micro-tasks.json",
    ]
    for idx in range(len(tasks)):
        refs.append(f"generated_sprint/{idx}-tasks.md")
    return refs


def process_stage(paths: WorkerPaths, stage: str, job: Dict[str, Any]) -> List[str]:
    refs: List[str]
    if stage == "factory-hopper":
        refs = process_factory_hopper(paths, job)
    elif stage == "writers-room":
        refs = process_writers(paths, job)
    elif stage == "observatory":
        refs = process_observatory(paths, job)
    elif stage == "sprint-planning-room":
        refs = process_sprint_planning(paths, job)
    elif stage == "prompt-engineering-lab":
        refs = process_prompt_engineering(paths, job)
    elif stage == "blueprint-forge":
        refs = process_blueprint_forge(paths, job)
    elif stage == "factory-floor":
        refs = process_factory_floor(paths, job)
    else:
        raise RuntimeError(f"Unsupported stage: {stage}")

    out_dir = paths.stages_root / stage / "outbox" / job["job_id"]
    refs.extend(_run_custom_agent_hook(paths, stage, job, out_dir))
    return refs


def worker_run_once(paths: WorkerPaths, stage: str) -> Dict[str, Any]:
    claimed = consume_one_job(paths, stage)
    if claimed is None:
        return {
            "ts": utc_now(),
            "stage": stage,
            "result": "idle",
            "queue_depth": 0,
        }

    package_dir, claim_info = claimed

    job = load_json(package_dir / PACKAGE_JOB_FILE)
    job["package_dir"] = str(package_dir)
    job["claim_info"] = claim_info
    work_ctx = _work_attempt_context(stage, job, claim_info)

    try:
        _emit_work_started(paths, work_ctx)
        _maybe_emit_work_heartbeat(paths, work_ctx, "start", force=True)
        refs = process_stage(paths, stage, job)
        _maybe_emit_work_heartbeat(paths, work_ctx, "after_process_stage")
        _run_contract_qa_gate(paths, stage, job, refs)
        _maybe_emit_work_heartbeat(paths, work_ctx, "after_qa_gate")
        _enforce_assignment_policy(paths, stage, job, refs)
        _maybe_emit_work_heartbeat(paths, work_ctx, "after_assignment_policy")
        _mark_success(paths, stage, job, refs, claim_info=claim_info)

        next_stage = NEXT_STAGE[stage]
        handoff = None
        if next_stage is not None:
            handoff = _handoff_package(paths, stage, next_stage, job, refs)
            _maybe_emit_work_heartbeat(paths, work_ctx, "after_handoff")

        _emit_work_terminal(
            paths,
            work_ctx,
            event_type="stage.work.completed",
            status="completed",
            reason="work_attempt_completed",
            artifact_refs=refs,
            payload={"next_stage": next_stage or ""},
        )

        archive_job(paths, stage, package_dir, "archive")

        return {
            "ts": utc_now(),
            "stage": stage,
            "result": "ok",
            "job_id": job["job_id"],
            "next_stage": next_stage,
            "artifact_refs": refs,
            "handoff": handoff,
            "claim": claim_info,
            "queue_depth": queue_depth(paths, stage),
        }
    except QAGateValidationError as exc:
        _emit_work_terminal(
            paths,
            work_ctx,
            event_type="stage.work.failed",
            status="failed",
            reason="qa_gate_validation_failed",
            payload={"error": str(exc), "details": exc.details},
        )
        _record_qa_gate_failure(paths, stage, job, exc)
        _mark_failure(paths, stage, job, str(exc))
        archive_job(paths, stage, package_dir, "failed")

        debug_dir = stage_dir(paths, stage, "debug", job.get("job_id", "unknown-job"))
        write_json(
            debug_dir / "error.json",
            {
                "ts": utc_now(),
                "stage": stage,
                "job": job,
                "error": str(exc),
                "error_details": exc.details,
            },
        )

        return {
            "ts": utc_now(),
            "stage": stage,
            "result": "failed",
            "job_id": job.get("job_id", "unknown"),
            "error": str(exc),
            "error_details": exc.details,
            "queue_depth": queue_depth(paths, stage),
        }
    except AssignmentPolicyValidationError as exc:
        _emit_work_terminal(
            paths,
            work_ctx,
            event_type="stage.work.failed",
            status="failed",
            reason="assignment_policy_validation_failed",
            payload={"error": str(exc), "details": exc.details},
        )
        _record_assignment_policy_failure(paths, stage, job, exc)
        _mark_failure(paths, stage, job, str(exc))
        archive_job(paths, stage, package_dir, "failed")

        debug_dir = stage_dir(paths, stage, "debug", job.get("job_id", "unknown-job"))
        write_json(
            debug_dir / "error.json",
            {
                "ts": utc_now(),
                "stage": stage,
                "job": job,
                "error": str(exc),
                "error_details": exc.details,
            },
        )

        return {
            "ts": utc_now(),
            "stage": stage,
            "result": "failed",
            "job_id": job.get("job_id", "unknown"),
            "error": str(exc),
            "error_details": exc.details,
            "queue_depth": queue_depth(paths, stage),
        }
    except HandoffValidationError as exc:
        _emit_work_terminal(
            paths,
            work_ctx,
            event_type="stage.work.failed",
            status="failed",
            reason="handoff_validation_failed",
            payload={"error": str(exc), "details": exc.details},
        )
        _record_handoff_validation_failure(paths, stage, job, exc)
        _mark_failure(paths, stage, job, str(exc))
        archive_job(paths, stage, package_dir, "failed")

        debug_dir = stage_dir(paths, stage, "debug", job.get("job_id", "unknown-job"))
        write_json(
            debug_dir / "error.json",
            {
                "ts": utc_now(),
                "stage": stage,
                "job": job,
                "error": str(exc),
                "error_details": exc.details,
            },
        )

        return {
            "ts": utc_now(),
            "stage": stage,
            "result": "failed",
            "job_id": job.get("job_id", "unknown"),
            "error": str(exc),
            "error_details": exc.details,
            "queue_depth": queue_depth(paths, stage),
        }
    except Exception as exc:
        _emit_work_terminal(
            paths,
            work_ctx,
            event_type="stage.work.failed",
            status="failed",
            reason="unhandled_stage_exception",
            payload={"error": str(exc), "error_type": type(exc).__name__},
        )
        _mark_failure(paths, stage, job, str(exc))
        archive_job(paths, stage, package_dir, "failed")

        debug_dir = stage_dir(paths, stage, "debug", job.get("job_id", "unknown-job"))
        write_json(
            debug_dir / "error.json",
            {
                "ts": utc_now(),
                "stage": stage,
                "job": job,
                "error": str(exc),
            },
        )

        return {
            "ts": utc_now(),
            "stage": stage,
            "result": "failed",
            "job_id": job.get("job_id", "unknown"),
            "error": str(exc),
            "queue_depth": queue_depth(paths, stage),
        }


def cmd_init(args: argparse.Namespace, paths: WorkerPaths) -> None:
    ensure_stage_layout(paths)
    print(json.dumps({"result": "ok", "message": f"Initialized layout at {paths.stages_root}"}, indent=2))


def cmd_enqueue(args: argparse.Namespace, paths: WorkerPaths) -> None:
    ensure_stage_layout(paths)
    project_slug = resolve_project_slug(paths, args.project)
    source_conv = pick_conversation(paths, project_slug, args.conversation)
    conversation_id = source_conv.parent.name
    job_id = f"{slugify(project_slug)}--{slugify(conversation_id)}"

    job = {
        "schema_version": SCHEMA_VERSION,
        "job_id": job_id,
        "project_slug": project_slug,
        "conversation_id": conversation_id,
        "conversation_path": str(source_conv),
        "created_at": utc_now(),
        "artifact_refs": [],
    }

    target = enqueue_job(paths, "factory-hopper", job)
    print(
        json.dumps(
            {
                "result": "ok",
                "stage": "factory-hopper",
                "job_id": job_id,
                "queued_job": str(target),
            },
            indent=2,
            ensure_ascii=True,
        )
    )


def cmd_worker(args: argparse.Namespace, paths: WorkerPaths) -> None:
    ensure_stage_layout(paths)

    if args.stage not in STAGE_NAMES:
        raise SystemExit(f"Unknown stage: {args.stage}")

    if args.once:
        print(json.dumps(worker_run_once(paths, args.stage), indent=2, ensure_ascii=True))
        return

    while True:
        print(json.dumps(worker_run_once(paths, args.stage), indent=2, ensure_ascii=True))
        time.sleep(args.interval_sec)


def cmd_worker_all_once(args: argparse.Namespace, paths: WorkerPaths) -> None:
    ensure_stage_layout(paths)
    results: List[Dict[str, Any]] = []
    for stage in STAGE_NAMES:
        results.append(worker_run_once(paths, stage))
    print(json.dumps({"result": "ok", "runs": results}, indent=2, ensure_ascii=True))


def _prompt_filename(role: str) -> str:
    if role == "coder":
        return "coder_prompt.md"
    if role == "qa":
        return "qa_prompt.md"
    if role == "architect":
        return "architect_prompt.md"
    raise SystemExit(f"Unknown role: {role}")


def _read_generated_prompt(paths: WorkerPaths, job_id: str, role: str) -> Tuple[Path, str]:
    filename = _prompt_filename(role)

    # Preferred lookup: package moved to the next stage inbox via ThePostalService.
    preferred = paths.stages_root / "blueprint-forge" / "inbox" / job_id / "agent_prompts" / filename
    if preferred.exists():
        return preferred, preferred.read_text(encoding="utf-8")

    # Fallback lookup: prompt stage archive if downstream stage already consumed the package.
    fallback_archive = paths.stages_root / "prompt-engineering-lab" / "archive" / job_id / "agent_prompts" / filename
    if fallback_archive.exists():
        return fallback_archive, fallback_archive.read_text(encoding="utf-8")

    # Last fallback: direct prompt stage outbox in case transport was bypassed.
    fallback_outbox = paths.stages_root / "prompt-engineering-lab" / "outbox" / job_id / "agent_prompts" / filename
    if fallback_outbox.exists():
        return fallback_outbox, fallback_outbox.read_text(encoding="utf-8")

    raise SystemExit(f"Prompt artifact not found for role={role}, job_id={job_id}")


def _read_assignment_plan(paths: WorkerPaths, job_id: str) -> Optional[Tuple[Path, Dict[str, Any]]]:
    candidates = [
        paths.stages_root / "factory-floor" / "outbox" / job_id / "generated_sprint" / "assignment_plan.json",
        paths.stages_root / "factory-floor" / "archive" / job_id / "generated_sprint" / "assignment_plan.json",
    ]
    for candidate in candidates:
        if candidate.exists():
            try:
                return candidate, load_json(candidate)
            except Exception:
                return None
    return None


def _scrum_signals_dir(paths: WorkerPaths) -> Path:
    # Keep Scrum-Master signal artifacts in one durable directory for review tooling.
    # This path is shared by both raised and cleared lifecycle documents.
    # Centralizing path logic avoids drift across emitters.
    return paths.hopper_root / "docs" / "scrum_master_signals"


def _open_blockers_path(paths: WorkerPaths) -> Path:
    # Registry stores open/cleared linkage so clear events only fire for real open blockers.
    # This prevents accidental clear emissions when no blocker was previously raised.
    # Review tooling can read this file for active blocker projections.
    return _scrum_signals_dir(paths) / "open_blockers.json"


def _load_open_blockers(paths: WorkerPaths) -> Dict[str, Any]:
    registry_path = _open_blockers_path(paths)
    if not registry_path.exists():
        return {"schema_version": SCHEMA_VERSION, "updated_at": utc_now(), "blockers": {}}
    try:
        doc = load_json(registry_path)
    except Exception:
        return {"schema_version": SCHEMA_VERSION, "updated_at": utc_now(), "blockers": {}}

    doc.setdefault("schema_version", SCHEMA_VERSION)
    doc.setdefault("updated_at", utc_now())
    blockers = doc.get("blockers", {})
    if not isinstance(blockers, dict):
        blockers = {}
    doc["blockers"] = blockers
    return doc


def _save_open_blockers(paths: WorkerPaths, registry: Dict[str, Any]) -> None:
    registry["schema_version"] = SCHEMA_VERSION
    registry["updated_at"] = utc_now()
    write_json(_open_blockers_path(paths), registry)


def _blocker_key(stage: str, job_id: str, reason: str) -> str:
    return f"{stage}:{job_id}:{slugify(reason)}"


def _blocker_policy(reason: str, requires_user_action: bool) -> Dict[str, str]:
    # Reason policy keeps severity/category stable across repeated emissions.
    # Unknown reasons default to informational so new reason strings remain non-breaking.
    # User-action blockers are always elevated to critical severity.
    policy = dict(BLOCKER_REASON_POLICY.get(reason, {"severity": "info", "category": "general"}))
    if requires_user_action:
        policy["severity"] = "critical"
    return policy


def _emit_scrum_master_clear(
    paths: WorkerPaths,
    stage: str,
    job_id: str,
    reason: str,
    details: Dict[str, Any],
) -> Optional[Dict[str, Any]]:
    # Clear events should only emit when a matching blocker is currently open.
    # Linking clear payloads to the original blocker id preserves causal auditability.
    # We keep clear artifacts alongside raised artifacts for review compatibility.
    blocker_key = _blocker_key(stage, job_id, reason)
    registry = _load_open_blockers(paths)
    blockers = registry.get("blockers", {})
    # Only clear blockers that are currently open in the registry.
    # This prevents false-clear artifacts when no matching raised signal exists.
    current = blockers.get(blocker_key)
    if not isinstance(current, dict) or current.get("status") != "open":
        return None

    blocker_id = str(current.get("blocker_id", "")) or f"blk-{slugify(job_id)}-{slugify(reason)}"
    clear_signal_id = f"{job_id}--{slugify(reason)}--cleared--{int(dt.datetime.now(tz=dt.timezone.utc).timestamp())}"
    clear_doc = {
        "schema_version": SCHEMA_VERSION,
        "event_kind": "cleared",
        "signal_id": clear_signal_id,
        "blocker_id": blocker_id,
        "blocker_key": blocker_key,
        "linked_signal_id": current.get("signal_id", ""),
        "linked_blocker_event_id": current.get("blocker_event_id", ""),
        "ts": utc_now(),
        "stage": stage,
        "job_id": job_id,
        "reason": reason,
        "requires_user_action": False,
        "details": details,
    }

    signal_path = _scrum_signals_dir(paths) / f"{clear_signal_id}.json"
    write_json(signal_path, clear_doc)

    clear_event = build_event_envelope(
        event_type="scrum.blocker.cleared",
        correlation_id=job_id,
        causation_id=str(current.get("blocker_event_id") or blocker_id),
        aggregate_type="task",
        aggregate_id=job_id,
        stage=stage,
        worker="TessScrumMaster",
        attempt=1,
        idempotency_key=f"{make_idempotency_key(job_id, stage, 1)}:scrum-blocker-cleared:{slugify(reason)}",
        status="completed",
        reason=reason,
        artifact_refs=[str(signal_path)],
        payload={
            "details": details,
            "signal_id": clear_signal_id,
            "blocker_id": blocker_id,
            "blocker_key": blocker_key,
        },
    )
    write_contract_event(paths, clear_event)

    event = stage_event(
        stage=stage,
        actor="TessScrumMaster",
        event="dispatch_interaction",
        summary=f"Scrum-Master blocker cleared: {reason}",
        artifact_refs=[str(signal_path)],
        status="ok",
        error_code="",
    )
    write_stage_chatroom(paths, stage, event)
    maybe_send_ws_event(paths, event)

    # Persist clear state so future emissions know this blocker is no longer active.
    # Keeping clear metadata in registry supports replay and review summarization.
    current["status"] = "cleared"
    current["cleared_at"] = utc_now()
    # Keep both signal and event ids so tooling can trace clear artifacts to stream entries.
    current["clear_signal_id"] = clear_signal_id
    current["clear_signal_path"] = str(signal_path)
    current["clear_event_id"] = clear_event["event_id"]
    current["clear_details"] = details
    # Save back into registry map as the final step of clear-state transition.
    blockers[blocker_key] = current
    registry["blockers"] = blockers
    _save_open_blockers(paths, registry)

    return {
        "signal_id": clear_signal_id,
        "signal_path": str(signal_path),
        "blocker_id": blocker_id,
        "blocker_key": blocker_key,
    }


def _emit_scrum_master_signal(
    paths: WorkerPaths,
    stage: str,
    job_id: str,
    reason: str,
    details: Dict[str, Any],
    requires_user_action: bool,
) -> Dict[str, Any]:
    # -------------------------------------------------------------------------
    # Tess Scrum-Master Signal
    # Emits a durable blocker/signal file and a live chatroom/ws event.
    # This keeps skipped/blocked flow visible beyond static artifacts.
    # -------------------------------------------------------------------------
    blocker_key = _blocker_key(stage, job_id, reason)
    policy = _blocker_policy(reason, requires_user_action)
    registry = _load_open_blockers(paths)
    blockers = registry.get("blockers", {})
    # Reuse blocker id for repeated reasons so clear links remain stable over time.
    # A stable blocker key avoids exploding duplicate blocker records per retry run.
    current = blockers.get(blocker_key, {}) if isinstance(blockers, dict) else {}
    blocker_id = str(current.get("blocker_id", "")) or (
        f"blk-{slugify(stage)}-{slugify(job_id)}-{slugify(reason)}"
    )

    signal_id = f"{job_id}--{slugify(reason)}--{int(dt.datetime.now(tz=dt.timezone.utc).timestamp())}"
    signal_doc = {
        "schema_version": SCHEMA_VERSION,
        "event_kind": "raised",
        "signal_id": signal_id,
        "blocker_id": blocker_id,
        "blocker_key": blocker_key,
        "ts": utc_now(),
        "stage": stage,
        "job_id": job_id,
        "reason": reason,
        "requires_user_action": requires_user_action,
        "severity": policy["severity"],
        "category": policy["category"],
        "details": details,
    }

    signal_path = _scrum_signals_dir(paths) / f"{signal_id}.json"
    write_json(signal_path, signal_doc)

    summary = f"Scrum-Master signal: {reason}"
    if requires_user_action:
        summary += " (user action requested)"

    event = stage_event(
        stage=stage,
        actor="TessScrumMaster",
        event="dispatch_interaction",
        summary=summary,
        artifact_refs=[str(signal_path)],
        status="blocked" if requires_user_action else "ok",
        error_code="scrum_blocker" if requires_user_action else "",
    )
    write_stage_chatroom(paths, stage, event)
    maybe_send_ws_event(paths, event)

    blocker_event = build_event_envelope(
        event_type="scrum.blocker.raised",
        correlation_id=job_id,
        causation_id=signal_id,
        aggregate_type="task",
        aggregate_id=job_id,
        stage=stage,
        worker="TessScrumMaster",
        attempt=1,
        idempotency_key=f"{make_idempotency_key(job_id, stage, 1)}:scrum-blocker-raised:{slugify(reason)}",
        status="blocked" if requires_user_action else "ok",
        reason=reason,
        requires_user_action=requires_user_action,
        artifact_refs=[str(signal_path)],
        payload={
            "details": details,
            "signal_id": signal_id,
            "blocker_id": blocker_id,
            "blocker_key": blocker_key,
            "severity": policy["severity"],
            "category": policy["category"],
        },
    )
    write_contract_event(paths, blocker_event)

    if requires_user_action:
        user_action_event = build_event_envelope(
            event_type="scrum.user_action.required",
            correlation_id=job_id,
            causation_id=blocker_event["event_id"],
            aggregate_type="task",
            aggregate_id=job_id,
            stage=stage,
            worker="TessScrumMaster",
            attempt=1,
            idempotency_key=f"{make_idempotency_key(job_id, stage, 1)}:scrum-user-action:{slugify(reason)}",
            status="blocked",
            reason=reason,
            requires_user_action=True,
            artifact_refs=[str(signal_path)],
            payload={
                "details": details,
                "signal_id": signal_id,
                "blocker_id": blocker_id,
                "blocker_key": blocker_key,
                "severity": policy["severity"],
                "category": policy["category"],
            },
        )
        write_contract_event(paths, user_action_event)

    blockers[blocker_key] = {
        # Registry entry tracks latest open state and linkage for clear semantics.
        # Review tooling uses this map to identify active blockers quickly.
        "blocker_id": blocker_id,
        "blocker_key": blocker_key,
        "signal_id": signal_id,
        "signal_path": str(signal_path),
        "stage": stage,
        "job_id": job_id,
        "reason": reason,
        "severity": policy["severity"],
        "category": policy["category"],
        "requires_user_action": requires_user_action,
        "status": "open",
        "opened_at": current.get("opened_at", utc_now()) if isinstance(current, dict) else utc_now(),
        "last_seen_at": utc_now(),
        "details": details,
        "raise_count": int(current.get("raise_count", 0)) + 1 if isinstance(current, dict) else 1,
        "blocker_event_id": blocker_event["event_id"],
    }
    registry["blockers"] = blockers
    _save_open_blockers(paths, registry)

    return {
        "signal_id": signal_id,
        "signal_path": str(signal_path),
        "requires_user_action": requires_user_action,
        "blocker_id": blocker_id,
        "blocker_key": blocker_key,
    }


def _resolve_dispatch_assignment(paths: WorkerPaths, job: Dict[str, Any], role: str) -> Dict[str, Any]:
    # Dispatch must be role-bound to assignment plan; fallback metadata is explicit.
    job_id = str(job.get("job_id", ""))
    resolved = _read_assignment_plan(paths, job_id)
    if resolved is None:
        return {
            "policy_enforced": False,
            "assignment_plan_path": "",
            "assignment": None,
            "skipped_assignments": [],
            "scrum_master_signal": None,
            "retry_metadata": {
                "attempt": 0,
                "max_attempts": ASSIGNMENT_RETRY_POLICY["max_attempts"],
                "fallback_role": "architect",
                "route_on_failure": ASSIGNMENT_RETRY_POLICY["terminal_route"],
            },
        }

    plan_path, plan_doc = resolved
    assignments = plan_doc.get("assignments", []) if isinstance(plan_doc, dict) else []
    if not isinstance(assignments, list):
        assignments = []

    skipped: List[Dict[str, Any]] = []
    selected: Optional[Dict[str, Any]] = None

    # Select the first matching role assignment in plan order (deterministic dispatch).
    for assignment in assignments:
        if not isinstance(assignment, dict):
            continue
        # Skip non-matching roles to preserve assignment-plan ordering semantics.
        if assignment.get("owner_role") != role:
            continue
        dispatch_status = str(assignment.get("dispatch_status", "ready")).strip().lower() or "ready"
        if dispatch_status != "ready":
            # Record skipped entries so Tess can surface non-ready blockers.
            skipped.append(
                {
                    "micro_task_id": assignment.get("micro_task_id", ""),
                    "owner_role": role,
                    "dispatch_status": dispatch_status,
                    "skip_reason": f"dispatch_status={dispatch_status}",
                }
            )
            continue
        # Return full assignment payload, including retry metadata for caller handling.
        selected = assignment
        break

    scrum_signal = None
    if skipped:
        # Non-ready assignments raise a warning signal even when one assignment is selectable.
        # This keeps partial dispatch friction visible without hard-blocking execution.
        scrum_signal = _emit_scrum_master_signal(
            paths=paths,
            stage="factory-floor",
            job_id=job_id,
            reason="one_or_more_assignments_skipped",
            details={"role": role, "skipped_assignments": skipped, "assignment_plan_path": str(plan_path)},
            requires_user_action=False,
        )
    else:
        # When no assignments are skipped, clear any stale skipped-assignment blocker.
        # This ensures open blocker views converge as readiness improves.
        _emit_scrum_master_clear(
            paths=paths,
            stage="factory-floor",
            job_id=job_id,
            reason="one_or_more_assignments_skipped",
            details={"role": role, "resolution": "all_assignments_ready", "assignment_plan_path": str(plan_path)},
        )

    if selected is not None:
        # Selecting a ready assignment resolves prior no-ready blockers for this role.
        # Clear emission is guarded by registry state, so this is safe if none were open.
        _emit_scrum_master_clear(
            paths=paths,
            stage="factory-floor",
            job_id=job_id,
            reason="no_ready_assignment_for_role",
            details={"role": role, "resolution": "ready_assignment_selected", "assignment_plan_path": str(plan_path)},
        )
        return {
            "policy_enforced": True,
            "assignment_plan_path": str(plan_path),
            "assignment": selected,
            "skipped_assignments": skipped,
            "scrum_master_signal": scrum_signal,
            "retry_metadata": selected.get("retry_metadata", {}),
        }

    blocker = _emit_scrum_master_signal(
        paths=paths,
        stage="factory-floor",
        job_id=job_id,
        reason="no_ready_assignment_for_role",
        details={"role": role, "skipped_assignments": skipped, "assignment_plan_path": str(plan_path)},
        requires_user_action=True,
    )

    raise SystemExit(
        f"No ready assignment found for role={role}, job_id={job_id}. "
        f"Scrum-Master signal: {blocker['signal_id']}. "
        f"Dispatch blocked by assignment policy."
    )


def cmd_next_prompt(args: argparse.Namespace, paths: WorkerPaths) -> None:
    ensure_stage_layout(paths)

    # Resolve the source first so this command can restart the run deterministically.
    project_slug = resolve_project_slug(paths, args.project)
    source_conv = pick_conversation(paths, project_slug, args.conversation)
    conversation_id = source_conv.parent.name
    job_id = f"{slugify(project_slug)}--{slugify(conversation_id)}"

    # Create the stage-1 package exactly like enqueue-conversation.
    job = {
        "schema_version": SCHEMA_VERSION,
        "job_id": job_id,
        "project_slug": project_slug,
        "conversation_id": conversation_id,
        "conversation_path": str(source_conv),
        "created_at": utc_now(),
        "artifact_refs": [],
    }
    enqueue_job(paths, "factory-hopper", job)

    # Run from beginning through prompt-engineering to generate role prompts.
    run_order = (
        # Ordered stage progression keeps prompt generation reproducible.
        "factory-hopper",
        "writers-room",
        "observatory",
        "sprint-planning-room",
        "prompt-engineering-lab",
    )
    runs: List[Dict[str, Any]] = []
    for stage in run_order:
        result = worker_run_once(paths, stage)
        # Keep per-stage result trail for operator visibility in command output.
        runs.append(result)
        if result.get("result") != "ok":
            raise SystemExit(f"Stage failed or idle during next-prompt run: {stage} => {result}")

    # Read the generated prompt from whichever package location exists now.
    prompt_path, prompt_text = _read_generated_prompt(paths, job_id, args.role)
    # Resolve assignment mapping so prompt dispatch is policy-compliant.
    assignment_dispatch = _resolve_dispatch_assignment(paths, job, args.role)

    # Enforce role prompt budget cap before returning prompt to caller.
    prompt_budget = ASSIGNMENT_ROLE_BUDGETS[args.role]["max_prompt_chars"]
    if len(prompt_text) > prompt_budget:
        blocker = _emit_scrum_master_signal(
            paths=paths,
            stage="factory-floor",
            job_id=job_id,
            reason="prompt_budget_exceeded",
            details={"role": args.role, "prompt_chars": len(prompt_text), "prompt_cap": prompt_budget},
            requires_user_action=True,
        )
        raise SystemExit(
            f"Prompt budget exceeded for role={args.role}: {len(prompt_text)} > {prompt_budget}. "
            f"Scrum-Master signal: {blocker['signal_id']}. "
            f"Dispatch blocked by assignment policy."
        )

    _emit_scrum_master_clear(
        paths=paths,
        stage="factory-floor",
        job_id=job_id,
        reason="prompt_budget_exceeded",
        details={"role": args.role, "resolution": "prompt_within_budget", "prompt_chars": len(prompt_text)},
    )

    print(
        json.dumps(
            {
                "result": "ok",
                "job_id": job_id,
                "project_slug": project_slug,
                "conversation_path": str(source_conv),
                "role": args.role,
                "prompt_path": str(prompt_path),
                "prompt_text": prompt_text,
                "assignment_dispatch": assignment_dispatch,
                "runs": runs,
            },
            indent=2,
            ensure_ascii=True,
        )
    )


def cmd_register_postal_profiles(args: argparse.Namespace, paths: WorkerPaths) -> None:
    ensure_stage_layout(paths)
    registry = Path(args.registry).expanduser().resolve()
    registry.mkdir(parents=True, exist_ok=True)

    written: List[str] = []
    for stage in STAGE_NAMES:
        profile_file = registry / f"arcane-{stage}.toml"
        profile = "\n".join(
            [
                f'service_name = "{stage}"',
                f'outbox_path = "{paths.stages_root / stage / "outbox"}"',
                f'inbox_path = "{paths.stages_root / stage / "inbox"}"',
                "",
            ]
        )
        profile_file.write_text(profile, encoding="utf-8")
        written.append(str(profile_file))

    print(json.dumps({"result": "ok", "registry": str(registry), "profiles": written}, indent=2, ensure_ascii=True))


def _is_comment_like_line(line: str) -> bool:
    stripped = line.strip()
    if not stripped:
        return False
    if stripped.startswith("#"):
        return True
    # Treat standalone triple-quoted lines as block-comment markers for guard purposes.
    return stripped in {'"""', "'''"} or stripped.startswith('"""') or stripped.startswith("'''")


def _is_code_like_line(line: str) -> bool:
    stripped = line.strip()
    if not stripped:
        return False
    if _is_comment_like_line(line):
        return False
    return True


def _scan_comment_density(lines: List[str], start_line: int, end_line: int, max_uncommented: int) -> List[Dict[str, Any]]:
    violations: List[Dict[str, Any]] = []
    consecutive_code = 0
    span_start = start_line
    bracket_depth = 0

    for line_no in range(start_line, end_line + 1):
        line = lines[line_no - 1]
        stripped = line.strip()

        # Blank lines split logical chunks, so restart the local density counter.
        if not stripped:
            consecutive_code = 0
            span_start = line_no + 1
            continue

        if _is_comment_like_line(line):
            consecutive_code = 0
            span_start = line_no + 1
            opens = stripped.count("(") + stripped.count("[") + stripped.count("{")
            closes = stripped.count(")") + stripped.count("]") + stripped.count("}")
            bracket_depth = max(0, bracket_depth + opens - closes)
            continue
        if not _is_code_like_line(line):
            continue

        # Continuation lines inside long literals/calls are excluded from density accounting.
        in_continuation = bracket_depth > 0
        if in_continuation:
            opens = stripped.count("(") + stripped.count("[") + stripped.count("{")
            closes = stripped.count(")") + stripped.count("]") + stripped.count("}")
            bracket_depth = max(0, bracket_depth + opens - closes)
            continue

        if consecutive_code == 0:
            span_start = line_no
        consecutive_code += 1

        if consecutive_code > max_uncommented:
            violations.append(
                {
                    "line": line_no,
                    "span_start": span_start,
                    "span_end": line_no,
                    "message": f"more than {max_uncommented} consecutive code lines without comment",
                }
            )

        opens = stripped.count("(") + stripped.count("[") + stripped.count("{")
        closes = stripped.count(")") + stripped.count("]") + stripped.count("}")
        bracket_depth = max(0, bracket_depth + opens - closes)

    return violations


def _find_function_ranges(file_path: Path, function_names: List[str]) -> Dict[str, Tuple[int, int]]:
    lines = file_path.read_text(encoding="utf-8").splitlines()
    ranges: Dict[str, Tuple[int, int]] = {}

    for idx, raw in enumerate(lines, start=1):
        stripped = raw.strip()
        for name in function_names:
            if stripped.startswith(f"def {name}("):
                start = idx
                end = len(lines)
                for tail in range(idx + 1, len(lines) + 1):
                    t = lines[tail - 1]
                    if t.startswith("def ") and not t.startswith(" "):
                        end = tail - 1
                        break
                ranges[name] = (start, end)
    return ranges


def cmd_comment_guard(args: argparse.Namespace, paths: WorkerPaths) -> None:
    # -------------------------------------------------------------------------
    # Comment Guard
    # Enforces minimum explanatory comment density on targeted critical paths.
    # Default scope avoids legacy-wide failures by checking new control logic.
    # -------------------------------------------------------------------------
    file_path = Path(args.file).expanduser().resolve()
    if not file_path.exists():
        raise SystemExit(f"File not found: {file_path}")

    lines = file_path.read_text(encoding="utf-8").splitlines()
    max_uncommented = int(args.max_uncommented)

    if args.scope == "full":
        ranges = {"full_file": (1, len(lines))}
    else:
        target_functions = [name.strip() for name in args.functions.split(",") if name.strip()]
        ranges = _find_function_ranges(file_path, target_functions)
        missing = [name for name in target_functions if name not in ranges]
        if missing:
            raise SystemExit(f"Target function(s) not found in file: {', '.join(missing)}")

    failures: Dict[str, List[Dict[str, Any]]] = {}
    for name, (start, end) in ranges.items():
        violations = _scan_comment_density(lines, start, end, max_uncommented=max_uncommented)
        if violations:
            failures[name] = violations

    result = {
        "result": "ok" if not failures else "failed",
        "file": str(file_path),
        "scope": args.scope,
        "max_uncommented": max_uncommented,
        "checked_ranges": ranges,
        "failures": failures,
    }
    print(json.dumps(result, indent=2, ensure_ascii=True))

    if failures:
        raise SystemExit(1)


def cmd_scrum_master_review(args: argparse.Namespace, paths: WorkerPaths) -> None:
    # -------------------------------------------------------------------------
    # Polyglot review bridge
    # Uses Bun/Node for Scrum-Master signal aggregation and prioritization.
    # Python only orchestrates runtime selection and process execution.
    # -------------------------------------------------------------------------
    review_script = paths.hopper_root / "bin" / "scrum_master_review.mjs"
    if not review_script.exists():
        raise SystemExit(f"Scrum-Master review script not found: {review_script}")

    runtime = shutil.which("bun") or shutil.which("node")
    if runtime is None:
        raise SystemExit("Neither bun nor node is available. Install bun or node to run scrum-master-review.")

    cmd = [runtime, str(review_script), str(paths.factory_root)]
    proc = subprocess.run(cmd, capture_output=True, text=True)
    if proc.stdout.strip():
        print(proc.stdout.strip())
    # Propagate JS-side failure details so operators can act quickly.
    if proc.returncode != 0:
        raise SystemExit(proc.stderr.strip() or "scrum-master-review failed")


def cmd_contract_event_smoke(args: argparse.Namespace, paths: WorkerPaths) -> None:
    event_type = str(args.event_type)
    stage = str(args.stage)
    aggregate_id = str(args.aggregate_id)
    attempt = max(1, int(args.attempt))
    correlation_id = str(args.correlation_id or aggregate_id)
    causation_id = str(args.causation_id or "smoke-seed")

    payload = build_event_envelope(
        event_type=event_type,
        correlation_id=correlation_id,
        causation_id=causation_id,
        aggregate_type="task",
        aggregate_id=aggregate_id,
        stage=stage,
        worker="StageWorkerSmoke",
        attempt=attempt,
        idempotency_key=make_idempotency_key(aggregate_id, stage, attempt),
        status=str(args.status).strip().lower(),
        reason=str(args.reason),
        requires_user_action=bool(args.requires_user_action),
        artifact_refs=[],
        payload={"source": "contract-event-smoke"},
    )

    publisher = _resolve_event_publisher(paths)
    try:
        publisher.publish(payload)
    except EventPublishError as exc:
        print(
            json.dumps(
                {
                    "result": "failed",
                    "error": str(exc),
                    "transient": exc.transient,
                    "publisher": publisher.describe(),
                },
                indent=2,
                ensure_ascii=True,
            )
        )
        raise SystemExit(1)

    print(
        json.dumps(
            {
                "result": "ok",
                "publisher": publisher.describe(),
                "event_id": payload["event_id"],
                "event_type": payload["event_type"],
                "correlation_id": payload["correlation_id"],
            },
            indent=2,
            ensure_ascii=True,
        )
    )


def _projection_state_path(paths: WorkerPaths) -> Path:
    # Projection state is persisted under docs so operators can inspect and diff rebuild outputs.
    # Keeping this path stable makes handoff and automation scripts straightforward.
    # Replay commands always target this file unless explicitly overridden in future tasks.
    return paths.hopper_root / "docs" / "event_projections" / "state.json"


def cmd_event_projections_rebuild(args: argparse.Namespace, paths: WorkerPaths) -> None:
    # Rebuild reads canonical events from stream start and rewrites projection state atomically.
    # This command is safe to run repeatedly because projections are derived, not source-of-truth.
    # Output includes summary counts for quick operator verification.
    projection_mode = _current_projection_mode(paths)
    if projection_mode == "legacy":
        print(
            json.dumps(
                {
                    "result": "ok",
                    "projection_mode": projection_mode,
                    "message": "Projection rebuild skipped because projection mode is legacy.",
                    "legacy_snapshot": _legacy_projection_snapshot(paths),
                },
                indent=2,
                ensure_ascii=True,
            )
        )
        return

    event_stream = _event_stream_path(paths)
    projection_state = build_projection_state(event_stream)
    output_path = _projection_state_path(paths)
    write_projection_state(output_path, projection_state)

    print(
        json.dumps(
            {
                "result": "ok",
                "projection_mode": projection_mode,
                "projection_path": str(output_path),
                "event_stream": str(event_stream),
                "event_count": projection_state.get("source", {}).get("event_count", 0),
                "open_blocker_count": projection_state.get("sprint", {}).get("open_blocker_count", 0),
                "active_task_count": projection_state.get("sprint", {}).get("active_task_count", 0),
                "health_status": projection_state.get("sprint", {}).get("health_status", "ok"),
            },
            indent=2,
            ensure_ascii=True,
        )
    )


def cmd_event_projections_show(args: argparse.Namespace, paths: WorkerPaths) -> None:
    # Show command reads persisted projection state for fast daily scrum inspection.
    # When --rebuild is set, it refreshes state first to avoid stale operator views.
    # Views are intentionally narrow so terminals stay readable during triage.
    output_path = _projection_state_path(paths)
    projection_mode = _current_projection_mode(paths)

    if projection_mode == "legacy":
        print(
            json.dumps(
                {
                    "result": "ok",
                    "projection_mode": projection_mode,
                    "view": "legacy",
                    "projection_path": str(output_path),
                    "payload": _legacy_projection_snapshot(paths),
                },
                indent=2,
                ensure_ascii=True,
            )
        )
        return

    # Rebuild mode gives operators a one-command refresh + inspect flow.
    # Non-rebuild mode keeps reads fast when repeatedly checking the same snapshot.
    if args.rebuild:
        projection_state = build_projection_state(_event_stream_path(paths))
        write_projection_state(output_path, projection_state)
        # Rebuild branch persists fresh state before any view-specific slicing.
    else:
        projection_state = load_projection_state(output_path)

    # View selector controls which read model slice is printed for operators.
    view = str(args.view)
    limit = max(1, int(args.limit))

    # Sprint view is the default operator snapshot for standups and quick triage.
    if view == "sprint":
        payload = projection_state.get("sprint", {})
    elif view == "blockers":
        # Blocker view is sorted by age during rebuild; limit keeps terminal output compact.
        blockers = projection_state.get("blockers", {}).get("open", [])
        payload = {"open": blockers[:limit], "total_open": len(blockers)}
    elif view == "tasks":
        # Task view shows most recently updated tasks first for handoff clarity.
        task_items = projection_state.get("tasks", {}).get("items", {})
        sorted_items = sorted(task_items.items(), key=lambda item: str(item[1].get("updated_at", "")), reverse=True)
        payload = {"items": dict(sorted_items[:limit]), "active_task_count": projection_state.get("tasks", {}).get("active_task_count", 0)}
    elif view == "workers":
        # Worker view highlights exhaustion-heavy workers so load balancing can be adjusted.
        workers = projection_state.get("workers", {}).get("items", {})
        sorted_workers = sorted(workers.items(), key=lambda item: float(item[1].get("exhaustion", 0.0)), reverse=True)
        payload = {"items": dict(sorted_workers[:limit]), "total_workers": len(workers)}
    else:
        # Full view is useful for exporting complete projection state to handoff artifacts.
        payload = projection_state

    print(
        json.dumps(
            {
                "result": "ok",
                "projection_mode": projection_mode,
                "view": view,
                "projection_path": str(output_path),
                "payload": payload,
            },
            indent=2,
            ensure_ascii=True,
        )
    )


def cmd_event_replay(args: argparse.Namespace, paths: WorkerPaths) -> None:
    # Replay command rebuilds projection state from a selectable stream window.
    # Start-line and max-events filters support targeted recovery after partial failures.
    # Rebuilt projections are persisted to the canonical projection path for continuity.
    projection_mode = _current_projection_mode(paths)
    if projection_mode == "legacy":
        print(
            json.dumps(
                {
                    "result": "ok",
                    "projection_mode": projection_mode,
                    "message": "Replay skipped because projection mode is legacy.",
                    "legacy_snapshot": _legacy_projection_snapshot(paths),
                },
                indent=2,
                ensure_ascii=True,
            )
        )
        return

    event_stream = _event_stream_path(paths)
    start_line = max(1, int(args.start_line))
    max_events = max(0, int(args.max_events))

    events = load_event_stream_events(
        event_stream,
        start_line=start_line,
        max_events=max_events,
    )

    projection_state = build_projection_state_from_events(
        events,
        source_stream=event_stream,
        source_filters={"start_line": start_line, "max_events": max_events},
    )
    output_path = _projection_state_path(paths)
    write_projection_state(output_path, projection_state)

    print(
        json.dumps(
            {
                "result": "ok",
                "projection_mode": projection_mode,
                "event_stream": str(event_stream),
                "projection_path": str(output_path),
                "source_filters": {"start_line": start_line, "max_events": max_events},
                "event_count": projection_state.get("source", {}).get("event_count", 0),
                "open_blocker_count": projection_state.get("sprint", {}).get("open_blocker_count", 0),
                "active_task_count": projection_state.get("sprint", {}).get("active_task_count", 0),
                "health_status": projection_state.get("sprint", {}).get("health_status", "ok"),
            },
            indent=2,
            ensure_ascii=True,
        )
    )


def cmd_event_run_summary(args: argparse.Namespace, paths: WorkerPaths) -> None:
    # Run summary helps operators recover context quickly after interruptions.
    # It combines projection health with latest correlation activity from event stream.
    # Optional rebuild mode refreshes state before summary generation.
    output_path = _projection_state_path(paths)
    projection_mode = _current_projection_mode(paths)

    if projection_mode == "legacy":
        print(
            json.dumps(
                {
                    "result": "ok",
                    "projection_mode": projection_mode,
                    "projection_path": str(output_path),
                    "legacy_snapshot": _legacy_projection_snapshot(paths),
                    "message": "Run summary uses legacy snapshot while projection mode is legacy.",
                },
                indent=2,
                ensure_ascii=True,
            )
        )
        return

    # Rebuild mode ensures summaries reflect the latest canonical event stream.
    # Cached mode is faster for repeated checks during the same operator session.
    if args.rebuild:
        projection_state = build_projection_state(_event_stream_path(paths))
        write_projection_state(output_path, projection_state)
        # Rebuild branch refreshes persisted state before summary slicing.
    else:
        projection_state = load_projection_state(output_path)

    # Group latest event per correlation id to spotlight the freshest run context.
    events = load_event_stream_events(_event_stream_path(paths))
    latest_by_correlation: Dict[str, Dict[str, Any]] = {}

    # Keep only the newest event per correlation id for compact run recency tracking.
    for event in events:
        correlation_id = str(event.get("correlation_id", "")).strip()
        if not correlation_id:
            continue

        # Compare timestamps as canonical ISO strings to preserve deterministic ordering.
        current = latest_by_correlation.get(correlation_id)
        if current is None or str(event.get("ts_utc", "")) >= str(current.get("ts_utc", "")):
            latest_by_correlation[correlation_id] = event

    latest_correlation = None
    if latest_by_correlation:
        # Lexicographic compare is safe because canonical timestamps are ISO-8601 UTC.
        latest_correlation = max(
            latest_by_correlation.values(),
            key=lambda item: str(item.get("ts_utc", "")),
        )

    # Incomplete attempts are tasks whose latest attempt has not reached a terminal event.
    open_blockers = projection_state.get("blockers", {}).get("open", [])
    tasks = projection_state.get("tasks", {}).get("items", {})
    incomplete_attempts: List[Dict[str, Any]] = []

    # Iterate latest attempt snapshots so stalled tasks can be surfaced in one list.
    for task_id, task_state in tasks.items():
        latest_attempt = str(task_state.get("latest_attempt", ""))
        attempt_state = task_state.get("attempts", {}).get(latest_attempt, {})
        terminal_event = str(attempt_state.get("terminal_event_type", ""))

        # Terminal attempts are complete from an operator triage perspective.
        if terminal_event in {"stage.work.completed", "stage.work.failed", "stage.work.skipped"}:
            continue

        # Keep concise fields only so summaries remain readable in terminal workflows.
        incomplete_attempts.append(
            {
                "task_id": task_id,
                "latest_attempt": latest_attempt,
                "last_event_type": attempt_state.get("last_event_type", ""),
                "last_reason": attempt_state.get("last_reason", ""),
                "updated_at": attempt_state.get("updated_at", ""),
            }
        )

    incomplete_attempts.sort(key=lambda item: str(item.get("updated_at", "")), reverse=True)
    # Limit output to keep summary readable while still exposing high-priority entries.
    limited_incomplete_attempts = incomplete_attempts[: max(1, int(args.limit))]

    # Output structure is intentionally stable for both humans and automation scripts.
    print(
        json.dumps(
            {
                "result": "ok",
                "projection_mode": projection_mode,
                "projection_path": str(output_path),
                "sprint": projection_state.get("sprint", {}),
                "latest_correlation": {
                    "correlation_id": latest_correlation.get("correlation_id", "") if latest_correlation else "",
                    "event_type": latest_correlation.get("event_type", "") if latest_correlation else "",
                    "stage": latest_correlation.get("stage", "") if latest_correlation else "",
                    "ts_utc": latest_correlation.get("ts_utc", "") if latest_correlation else "",
                },
                "open_blockers": {
                    "count": len(open_blockers),
                    "items": open_blockers[: max(1, int(args.limit))],
                },
                "incomplete_attempts": {
                    "count": len(incomplete_attempts),
                    "items": limited_incomplete_attempts,
                },
            },
            indent=2,
            ensure_ascii=True,
        )
    )


def cmd_event_integrity_check(args: argparse.Namespace, paths: WorkerPaths) -> None:
    # Integrity check validates canonical envelopes on recent or full stream slices.
    # Violations are reported with stream line and event id for fast remediation.
    # Non-zero exit enforces fail-fast behavior for CI and operator diagnostics.
    event_stream = _event_stream_path(paths)
    start_line = max(1, int(args.start_line))
    max_events = max(0, int(args.max_events))

    # Range filters let operators focus integrity scans on recent post-restart windows.
    events = load_event_stream_events(event_stream, start_line=start_line, max_events=max_events)

    violations: List[Dict[str, Any]] = []

    # Validate each envelope independently so one failure does not hide subsequent issues.
    for event in events:
        try:
            validate_event_envelope(event)
        except EventSchemaValidationError as exc:
            # Keep stream line metadata so operators can jump straight to malformed records.
            violations.append(
                {
                    "stream_line": int(event.get("_stream_line", 0)),
                    "event_id": str(event.get("event_id", "")),
                    "event_type": str(event.get("event_type", "")),
                    "error": str(exc),
                }
            )

    result_payload = {
        # Result payload is structured for CI parsing and human terminal inspection.
        "result": "ok" if not violations else "failed",
        "event_stream": str(event_stream),
        "source_filters": {"start_line": start_line, "max_events": max_events},
        "checked_event_count": len(events),
        "violation_count": len(violations),
        "violations": violations,
    }
    print(json.dumps(result_payload, indent=2, ensure_ascii=True))

    if violations:
        raise SystemExit(1)


def cmd_event_contract_self_test(args: argparse.Namespace, paths: WorkerPaths) -> None:
    # Self-test command runs Task 8 fixture checks for contract conformance and idempotency.
    # Output is always JSON so automation and handoff agents can parse failures deterministically.
    # Non-zero exit on failures enforces fail-fast behavior in CI and manual validation loops.
    result = arcane_event_contract_self_tests.run_event_contract_self_tests()
    print(json.dumps(result, indent=2, ensure_ascii=True))
    if result.get("result") != "ok":
        raise SystemExit(1)


def cmd_event_rollout_status(args: argparse.Namespace, paths: WorkerPaths) -> None:
    # Rollout status surfaces current emit/projection modes and phase label.
    # Parity summary is included so operators can evaluate verify gate quickly.
    # Output remains JSON for scriptability in handoff and CI contexts.
    config = _load_rollout_config(paths)
    parity = _build_rollout_parity_report(paths)
    print(
        json.dumps(
            {
                "result": "ok",
                "rollout_config": config,
                "parity": {
                    "parity_ok": parity.get("parity_ok", False),
                    "legacy_signal_count": parity.get("legacy_signal_count", 0),
                    "contract_blocker_count": parity.get("contract_blocker_count", 0),
                    "contract_user_action_count": parity.get("contract_user_action_count", 0),
                },
            },
            indent=2,
            ensure_ascii=True,
        )
    )


def cmd_event_rollout_set(args: argparse.Namespace, paths: WorkerPaths) -> None:
    # Set command allows explicit phase transitions and rollback-friendly overrides.
    # Phase shortcuts reduce operator mistakes during repetitive cutover workflows.
    # Result payload echoes normalized config for reliable shell automation.
    # Start from existing persisted config so partial overrides remain stable.
    config = _load_rollout_config(paths)

    # Phase presets set both emit and projection behavior for safe staged rollout.
    phase = str(args.phase).strip().lower() if args.phase else ""
    if phase:
        if phase == "shadow":
            config["phase"] = "shadow_emit"
            config["emit_mode"] = "shadow"
            config["projection_mode"] = "shadow"
        elif phase == "verify":
            config["phase"] = "verify"
            config["emit_mode"] = "shadow"
            config["projection_mode"] = "shadow"
        elif phase == "enforce":
            config["phase"] = "enforce"
            config["emit_mode"] = "contract_only"
            config["projection_mode"] = "enforce"
        elif phase == "rollback_legacy":
            config["phase"] = "rollback_legacy"
            config["emit_mode"] = "legacy_only"
            config["projection_mode"] = "legacy"
        else:
            raise SystemExit("Invalid phase. Use one of: shadow, verify, enforce, rollback_legacy")

    # Explicit mode flags can override phase defaults for operator hotfix workflows.
    if args.emit_mode:
        config["emit_mode"] = str(args.emit_mode).strip().lower()
    if args.projection_mode:
        config["projection_mode"] = str(args.projection_mode).strip().lower()

    # Persist normalized config then echo final effective values.
    _save_rollout_config(paths, config)
    print(json.dumps({"result": "ok", "rollout_config": _load_rollout_config(paths)}, indent=2, ensure_ascii=True))


def cmd_event_rollout_verify_parity(args: argparse.Namespace, paths: WorkerPaths) -> None:
    # Verify command computes full parity report and can persist it for handoff artifacts.
    # Non-zero exit on parity mismatch allows fail-fast enforcement gating.
    # Report file path is explicit so runbooks can reference one stable artifact.
    report = _build_rollout_parity_report(paths)

    report_path = paths.hopper_root / "docs" / "event_rollout" / "parity_report.json"
    if args.write_report:
        write_json(report_path, report)

    print(
        json.dumps(
            {
                "result": "ok" if report.get("parity_ok", False) else "failed",
                "parity_report": report,
                "report_path": str(report_path),
            },
            indent=2,
            ensure_ascii=True,
        )
    )

    if args.fail_on_mismatch and not report.get("parity_ok", False):
        raise SystemExit(1)


def cmd_event_rollout_handoff(args: argparse.Namespace, paths: WorkerPaths) -> None:
    # Handoff package captures rollout mode, parity state, and execution health in one artifact.
    # This ensures another agent can resume without reconstructing operational context.
    # Markdown output is chosen for readability in docs and PR reviews.
    # Collect all status slices up front so package reflects one coherent point-in-time view.
    config = _load_rollout_config(paths)
    parity = _build_rollout_parity_report(paths)
    projection_mode = _current_projection_mode(paths)
    event_stream = _event_stream_path(paths)
    contract_events = load_event_stream_events(event_stream)
    projection_state = build_projection_state(event_stream) if projection_mode != "legacy" else None
    open_blocker_count = (
        int(projection_state.get("sprint", {}).get("open_blocker_count", 0)) if isinstance(projection_state, dict) else 0
    )

    # Risk list is intentionally explicit so the next operator has clear ownership.
    risks: List[Dict[str, str]] = []
    if not parity.get("parity_ok", False):
        risks.append(
            {
                "risk": "Legacy vs canonical Scrum signal parity mismatch",
                "mitigation_owner": "TessScrumMaster",
                "next_action": "Run event-rollout-verify-parity and inspect reason_parity deltas before enforcement",
            }
        )
    if projection_mode == "legacy":
        risks.append(
            {
                "risk": "Projection mode still legacy",
                "mitigation_owner": "FactoryOperator",
                "next_action": "Switch projection mode to shadow/enforce before contract-only cutover",
            }
        )
    if open_blocker_count > 0:
        risks.append(
            {
                "risk": f"{open_blocker_count} open blockers in projection state",
                "mitigation_owner": "TessScrumMaster",
                "next_action": "Resolve or clear blockers before final rollout gate",
            }
        )

    # Next commands provide deterministic continuation steps for handoff agents.
    next_commands = [
        "python stage_workers.py event-rollout-status",
        "python stage_workers.py event-rollout-verify-parity --write-report --fail-on-mismatch",
        "python stage_workers.py event-integrity-check",
        "python stage_workers.py event-run-summary --rebuild",
    ]

    handoff_payload = {
        "schema_version": SCHEMA_VERSION,
        "generated_at": utc_now(),
        "rollout_config": config,
        "parity": parity,
        "projection_mode": projection_mode,
        "contract_event_count": len(contract_events),
        "open_blocker_count": open_blocker_count,
        "risks": risks,
        "next_commands": next_commands,
    }

    output_dir = paths.hopper_root / "docs" / "event_rollout"
    output_dir.mkdir(parents=True, exist_ok=True)
    json_path = output_dir / "handoff_package.json"
    md_path = output_dir / "handoff_package.md"
    write_json(json_path, handoff_payload)

    # Markdown variant is optimized for human scan speed during shift handoff.
    lines = [
        "# Event Rollout Handoff Package",
        "",
        f"Generated at: {handoff_payload['generated_at']}",
        "",
        "## Rollout Status",
        "",
        f"- phase: {config.get('phase', '')}",
        f"- emit_mode: {config.get('emit_mode', '')}",
        f"- projection_mode: {config.get('projection_mode', '')}",
        f"- parity_ok: {parity.get('parity_ok', False)}",
        f"- contract_event_count: {len(contract_events)}",
        f"- open_blocker_count: {open_blocker_count}",
        "",
        "## Risks",
        "",
    ]

    if risks:
        for item in risks:
            lines.append(f"- risk: {item['risk']}")
            lines.append(f"  owner: {item['mitigation_owner']}")
            lines.append(f"  next_action: {item['next_action']}")
    else:
        lines.append("- none")

    lines.extend(["", "## Next Commands", ""])
    for cmd in next_commands:
        lines.append(f"- {cmd}")

    md_path.write_text("\n".join(lines) + "\n", encoding="utf-8")
    print(
        json.dumps(
            {
                "result": "ok",
                "handoff_json": str(json_path),
                "handoff_markdown": str(md_path),
                "risk_count": len(risks),
            },
            indent=2,
            ensure_ascii=True,
        )
    )


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Run queue-driven Arcane Arcade Machine Factory workers.")
    parser.add_argument(
        "--factory-root",
        default=str(Path(__file__).resolve().parents[2]),
        help="Factory root path (defaults to ArcaneArcadeMachineFactory).",
    )

    subparsers = parser.add_subparsers(dest="command", required=True)

    init_parser = subparsers.add_parser("init-layout", help="Create stage directories.")
    init_parser.set_defaults(handler=cmd_init)

    enqueue_parser = subparsers.add_parser("enqueue-conversation", help="Queue a source conversation in factory-hopper inbox.")
    enqueue_parser.add_argument("--project", default=None, help="Optional project folder name override.")
    enqueue_parser.add_argument("--conversation", default=None, help="Optional explicit conversation.json path.")
    enqueue_parser.set_defaults(handler=cmd_enqueue)

    worker_parser = subparsers.add_parser("worker", help="Run one stage worker loop.")
    worker_parser.add_argument("--stage", required=True, help="Stage name (factory-hopper .. factory-floor).")
    worker_parser.add_argument("--once", action="store_true", help="Process only one job and exit.")
    worker_parser.add_argument("--interval-sec", type=float, default=5.0, help="Loop interval for worker mode.")
    worker_parser.set_defaults(handler=cmd_worker)

    worker_all_parser = subparsers.add_parser("worker-all-once", help="Run one pass across all stages.")
    worker_all_parser.set_defaults(handler=cmd_worker_all_once)

    register_parser = subparsers.add_parser(
        "register-postal-profiles",
        help="Write stage service profiles for ThePostalService registry.",
    )
    register_parser.add_argument(
        "--registry",
        default="/home/craigpar/Code/ThePostalService/registry",
        help="Target ThePostalService registry folder.",
    )
    register_parser.set_defaults(handler=cmd_register_postal_profiles)

    next_prompt_parser = subparsers.add_parser(
        "next-prompt",
        help="Restart from stage 1 and generate a role prompt through prompt-engineering stage.",
    )
    next_prompt_parser.add_argument("--project", default=None, help="Optional project folder name override.")
    next_prompt_parser.add_argument("--conversation", default=None, help="Optional explicit conversation.json path.")
    next_prompt_parser.add_argument(
        "--role",
        choices=["coder", "qa", "architect"],
        default="coder",
        help="Prompt role to return.",
    )
    next_prompt_parser.set_defaults(handler=cmd_next_prompt)

    comment_guard_parser = subparsers.add_parser(
        "comment-guard",
        help="Check comment quality guard on targeted control-path functions or full file.",
    )
    comment_guard_parser.add_argument(
        "--file",
        default=str(Path(__file__).resolve()),
        help="Python file to inspect for comment density guard.",
    )
    comment_guard_parser.add_argument(
        "--scope",
        choices=["targeted", "full"],
        default="targeted",
        help="Guard scope: targeted critical functions (default) or full file.",
    )
    comment_guard_parser.add_argument(
        "--functions",
        default="_enforce_assignment_policy,_run_contract_qa_gate,_resolve_dispatch_assignment,_resolve_event_publisher,_emit_claim_requested,_emit_claim_granted,_emit_work_started,_maybe_emit_work_heartbeat,_emit_work_terminal,_emit_scrum_master_signal,_emit_scrum_master_clear,_projection_state_path,cmd_event_projections_rebuild,cmd_event_projections_show,cmd_event_replay,cmd_event_run_summary,cmd_event_integrity_check,cmd_event_contract_self_test,cmd_next_prompt",
        help="Comma-separated function names used when --scope=targeted.",
    )
    comment_guard_parser.add_argument(
        "--max-uncommented",
        type=int,
        default=4,
        help="Maximum consecutive code lines without comment in guarded ranges.",
    )
    comment_guard_parser.set_defaults(handler=cmd_comment_guard)

    scrum_review_parser = subparsers.add_parser(
        "scrum-master-review",
        help="Summarize Scrum-Master interaction signals (polyglot Bun/Node runtime).",
    )
    scrum_review_parser.set_defaults(handler=cmd_scrum_master_review)

    contract_event_smoke_parser = subparsers.add_parser(
        "contract-event-smoke",
        help="Emit one canonical contract event through the configured publisher adapter.",
    )
    contract_event_smoke_parser.add_argument(
        "--event-type",
        default="scrum.blocker.raised",
        help="Canonical event type to emit.",
    )
    contract_event_smoke_parser.add_argument(
        "--stage",
        default="factory-floor",
        help="Stage name for emitted event.",
    )
    contract_event_smoke_parser.add_argument(
        "--aggregate-id",
        default="smoke-task",
        help="Aggregate identifier for event envelope.",
    )
    contract_event_smoke_parser.add_argument(
        "--attempt",
        type=int,
        default=1,
        help="Attempt number used in idempotency key.",
    )
    contract_event_smoke_parser.add_argument(
        "--status",
        default="ok",
        help="Event status value.",
    )
    contract_event_smoke_parser.add_argument(
        "--reason",
        default="smoke-test",
        help="Reason text for smoke event payload.",
    )
    contract_event_smoke_parser.add_argument(
        "--requires-user-action",
        action="store_true",
        help="Set requires_user_action=true on emitted event.",
    )
    contract_event_smoke_parser.add_argument(
        "--correlation-id",
        default="",
        help="Optional correlation id override.",
    )
    contract_event_smoke_parser.add_argument(
        "--causation-id",
        default="",
        help="Optional causation id override.",
    )
    contract_event_smoke_parser.set_defaults(handler=cmd_contract_event_smoke)

    event_proj_rebuild_parser = subparsers.add_parser(
        "event-projections-rebuild",
        help="Rebuild blocker/task/worker/sprint projections from canonical event stream.",
    )
    event_proj_rebuild_parser.set_defaults(handler=cmd_event_projections_rebuild)

    event_proj_show_parser = subparsers.add_parser(
        "event-projections-show",
        help="Show persisted projection views for blockers, tasks, workers, or sprint health.",
    )
    event_proj_show_parser.add_argument(
        "--view",
        choices=["all", "blockers", "tasks", "workers", "sprint"],
        default="sprint",
        help="Projection view to print.",
    )
    event_proj_show_parser.add_argument(
        "--limit",
        type=int,
        default=10,
        help="Maximum items for list-based views.",
    )
    event_proj_show_parser.add_argument(
        "--rebuild",
        action="store_true",
        help="Rebuild projections before showing selected view.",
    )
    event_proj_show_parser.set_defaults(handler=cmd_event_projections_show)

    event_replay_parser = subparsers.add_parser(
        "event-replay",
        help="Replay canonical events and rebuild projections with optional range filters.",
    )
    event_replay_parser.add_argument(
        "--start-line",
        type=int,
        default=1,
        help="1-based stream line where replay begins.",
    )
    event_replay_parser.add_argument(
        "--max-events",
        type=int,
        default=0,
        help="Maximum number of events to replay (0 means no limit).",
    )
    event_replay_parser.set_defaults(handler=cmd_event_replay)

    event_run_summary_parser = subparsers.add_parser(
        "event-run-summary",
        help="Show latest correlation summary with open blockers and incomplete attempts.",
    )
    event_run_summary_parser.add_argument(
        "--limit",
        type=int,
        default=10,
        help="Maximum rows for blockers and incomplete attempts.",
    )
    event_run_summary_parser.add_argument(
        "--rebuild",
        action="store_true",
        help="Rebuild projections before generating summary.",
    )
    event_run_summary_parser.set_defaults(handler=cmd_event_run_summary)

    event_integrity_parser = subparsers.add_parser(
        "event-integrity-check",
        help="Validate canonical event envelopes and report schema violations.",
    )
    event_integrity_parser.add_argument(
        "--start-line",
        type=int,
        default=1,
        help="1-based stream line where integrity scan begins.",
    )
    event_integrity_parser.add_argument(
        "--max-events",
        type=int,
        default=0,
        help="Maximum number of events to validate (0 means no limit).",
    )
    event_integrity_parser.set_defaults(handler=cmd_event_integrity_check)

    event_self_test_parser = subparsers.add_parser(
        "event-contract-self-test",
        help="Run Task8 event contract conformance and idempotency fixture suite.",
    )
    event_self_test_parser.set_defaults(handler=cmd_event_contract_self_test)

    event_rollout_status_parser = subparsers.add_parser(
        "event-rollout-status",
        help="Show Task9 rollout modes and current parity summary.",
    )
    event_rollout_status_parser.set_defaults(handler=cmd_event_rollout_status)

    event_rollout_set_parser = subparsers.add_parser(
        "event-rollout-set",
        help="Set rollout phase or explicit emit/projection modes for staged cutover.",
    )
    event_rollout_set_parser.add_argument(
        "--phase",
        choices=["shadow", "verify", "enforce", "rollback_legacy"],
        default="",
        help="Optional rollout phase shortcut.",
    )
    event_rollout_set_parser.add_argument(
        "--emit-mode",
        choices=sorted(ROLLOUT_EMIT_MODES),
        default="",
        help="Optional explicit emit mode override.",
    )
    event_rollout_set_parser.add_argument(
        "--projection-mode",
        choices=sorted(ROLLOUT_PROJECTION_MODES),
        default="",
        help="Optional explicit projection mode override.",
    )
    event_rollout_set_parser.set_defaults(handler=cmd_event_rollout_set)

    event_rollout_parity_parser = subparsers.add_parser(
        "event-rollout-verify-parity",
        help="Compare legacy Scrum signals and canonical blocker events for cutover gating.",
    )
    event_rollout_parity_parser.add_argument(
        "--write-report",
        action="store_true",
        help="Write parity report to docs/event_rollout/parity_report.json.",
    )
    event_rollout_parity_parser.add_argument(
        "--fail-on-mismatch",
        action="store_true",
        help="Exit non-zero when reason-level parity fails.",
    )
    event_rollout_parity_parser.set_defaults(handler=cmd_event_rollout_verify_parity)

    event_rollout_handoff_parser = subparsers.add_parser(
        "event-rollout-handoff",
        help="Publish Task9 rollout handoff package with status, risks, and next commands.",
    )
    event_rollout_handoff_parser.set_defaults(handler=cmd_event_rollout_handoff)

    return parser


def main() -> None:
    parser = build_parser()
    args = parser.parse_args()

    factory_root = Path(args.factory_root).expanduser().resolve()
    hopper_root = factory_root / "TheFactoryHopper"
    stages_root = resolve_stages_root(factory_root)

    paths = WorkerPaths(
        factory_root=factory_root,
        hopper_root=hopper_root,
        stages_root=stages_root,
        source_inbox=hopper_root / "inbox",
        shared_chatroom_audit=hopper_root / "chatroom_ws_audit" / "factory.chatroom.jsonl",
        active_project_file=hopper_root / "docs" / "active_project.json",
        stage_status_file=hopper_root / "docs" / "stage_status.json",
    )

    args.handler(args, paths)


if __name__ == "__main__":
    main()
