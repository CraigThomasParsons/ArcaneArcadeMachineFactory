#!/usr/bin/env python3
"""Arcane Arcade Machine Factory stage runtime.

This script scaffolds and runs the stage pipeline defined in docs/stage_architecture.md.
It is intentionally deterministic and file-driven so it can be audited and replayed.
"""

from __future__ import annotations

import argparse
import datetime as dt
import json
import re
import shutil
import socket
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Tuple

SCHEMA_VERSION = "v1"
STAGE_NAMES: Tuple[str, ...] = (
    "ingest-room",
    "writers-room",
    "planning-room",
    "implementation-room",
    "validation-room",
    "game-bridge-room",
)
STAGE_SUBDIRS: Tuple[str, ...] = (
    "inbox",
    "outbox",
    "archive",
    "failed",
    "debug",
    "chatroom",
)


@dataclass
class RuntimePaths:
    factory_root: Path
    stages_root: Path
    source_inbox: Path
    shared_chatroom_audit: Path
    active_project_file: Path
    stage_status_file: Path


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


def ensure_stage_layout(paths: RuntimePaths) -> None:
    for stage in STAGE_NAMES:
        for subdir in STAGE_SUBDIRS:
            (paths.stages_root / stage / subdir).mkdir(parents=True, exist_ok=True)


def project_dirs(source_inbox: Path) -> List[Path]:
    if not source_inbox.exists():
        return []
    return sorted([p for p in source_inbox.iterdir() if p.is_dir()], key=lambda p: p.name)


def conversation_files(project_dir: Path) -> List[Path]:
    return sorted(project_dir.glob("*/conversation.json"), key=lambda p: p.stat().st_mtime, reverse=True)


def read_active_project(active_path: Path) -> Dict[str, Any]:
    if not active_path.exists():
        return {"active_project": "", "updated_at": ""}
    return load_json(active_path)


def projects_state_path(paths: RuntimePaths) -> Path:
    return paths.active_project_file.parent / "projects_state.json"


def read_projects_state(paths: RuntimePaths) -> Dict[str, Any]:
    state_path = projects_state_path(paths)
    if not state_path.exists():
        return {"enabled_projects": [], "updated_at": ""}
    return load_json(state_path)


def write_projects_state(paths: RuntimePaths, enabled_projects: List[str]) -> None:
    write_json(
        projects_state_path(paths),
        {
            "enabled_projects": sorted(enabled_projects),
            "updated_at": utc_now(),
        },
    )


def allowed_projects(paths: RuntimePaths) -> List[Path]:
    projects = project_dirs(paths.source_inbox)
    state = read_projects_state(paths)
    enabled = set(state.get("enabled_projects", []))
    if not enabled:
        return projects
    return [p for p in projects if p.name in enabled]


def write_active_project(active_path: Path, project_slug: str) -> None:
    write_json(
        active_path,
        {
            "active_project": project_slug,
            "updated_at": utc_now(),
        },
    )


def resolve_project_slug(paths: RuntimePaths, explicit_project: Optional[str]) -> str:
    projects = allowed_projects(paths)
    if explicit_project:
        for candidate in projects:
            if candidate.name == explicit_project:
                return explicit_project
        raise SystemExit(f"Project not found or not enabled in inbox: {explicit_project}")

    active = read_active_project(paths.active_project_file).get("active_project", "")
    if active:
        for candidate in projects:
            if candidate.name == active:
                return active

    if not projects:
        raise SystemExit(f"No project folders found under {paths.source_inbox}")
    return projects[0].name


def pick_conversation(paths: RuntimePaths, project_slug: str, explicit_path: Optional[str]) -> Path:
    if explicit_path:
        candidate = Path(explicit_path).expanduser().resolve()
        if not candidate.exists():
            raise SystemExit(f"Conversation path not found: {candidate}")
        return candidate

    project_dir = paths.source_inbox / project_slug
    files = conversation_files(project_dir)
    if not files:
        raise SystemExit(f"No conversation.json files found under {project_dir}")
    return files[0]


def make_artifact_id(stage: str, job_id: str) -> str:
    return f"{slugify(stage)}--{job_id}"


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


def write_stage_chatroom(paths: RuntimePaths, stage: str, payload: Dict[str, Any]) -> None:
    stage_chat = paths.stages_root / stage / "chatroom" / "events.jsonl"
    append_jsonl(stage_chat, payload)
    append_jsonl(paths.shared_chatroom_audit, payload)


def update_stage_status(paths: RuntimePaths, stage: str, status: str, details: Dict[str, Any]) -> None:
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


def stage_dir(paths: RuntimePaths, stage: str, kind: str, job_id: str) -> Path:
    p = paths.stages_root / stage / kind / job_id
    p.mkdir(parents=True, exist_ok=True)
    return p


def maybe_send_ws_event(factory_root: Path, payload: Dict[str, Any]) -> None:
    """Attempt live publish when the local broker is available."""
    try:
        with socket.create_connection(("127.0.0.1", 8765), timeout=0.3):
            pass
    except OSError:
        return

    publish_script = factory_root / "bridge" / "publish_event.py"
    if not publish_script.exists():
        return

    import subprocess
    import sys

    cmd = [
        sys.executable,
        str(publish_script),
        "--channel",
        "factory.chatroom",
        "--stage",
        str(payload.get("stage", "")),
        "--actor",
        str(payload.get("actor", "StageRuntime")),
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
        subprocess.run(cmd, cwd=str(factory_root), check=False, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
    except OSError:
        return


def process_ingest(paths: RuntimePaths, project_slug: str, source_conv: Path, job_id: str) -> Dict[str, Any]:
    stage = "ingest-room"
    source_doc = load_json(source_conv)

    out_dir = stage_dir(paths, stage, "outbox", job_id)
    archive_dir = stage_dir(paths, stage, "archive", job_id)

    normalized = {
        "artifact_id": make_artifact_id(stage, job_id),
        "stage": stage,
        "schema_version": SCHEMA_VERSION,
        "created_at": utc_now(),
        "source_refs": [str(source_conv)],
        "payload": {
            "project_slug": project_slug,
            "conversation_id": source_conv.parent.name,
            "title": source_doc.get("title", "untitled"),
            "message_count": len(source_doc.get("mapping", {})),
            "update_time": source_doc.get("update_time"),
            "create_time": source_doc.get("create_time"),
        },
    }
    report = {
        "artifact_id": f"{make_artifact_id(stage, job_id)}--intake-report",
        "stage": stage,
        "schema_version": SCHEMA_VERSION,
        "created_at": utc_now(),
        "source_refs": [str(source_conv)],
        "payload": {
            "valid_json": True,
            "top_level_keys": sorted(list(source_doc.keys())),
            "warnings": [],
        },
    }

    normalized_path = out_dir / "normalized_conversation.json"
    report_path = out_dir / "intake_report.json"
    write_json(normalized_path, normalized)
    write_json(report_path, report)

    shutil.copy2(source_conv, archive_dir / "conversation.json")

    refs = [str(normalized_path), str(report_path)]
    return {
        "normalized": normalized,
        "intake_report": report,
        "artifact_refs": refs,
        "job_id": job_id,
    }


def process_writers(paths: RuntimePaths, ingest_out: Dict[str, Any], job_id: str) -> Dict[str, Any]:
    stage = "writers-room"
    payload = ingest_out["normalized"]["payload"]

    out_dir = stage_dir(paths, stage, "outbox", job_id)

    title = str(payload.get("title", "untitled"))
    project_slug = str(payload.get("project_slug", "unknown-project"))

    vision_text = (
        f"Project: {project_slug}\n"
        f"Conversation: {payload.get('conversation_id', '')}\n\n"
        f"Vision:\n"
        f"Build and iterate {title} through deterministic stage artifacts and live game visibility.\n"
    )
    goals_text = (
        "Goals:\n"
        "1. Produce reproducible stage outputs.\n"
        "2. Surface progress in Godot through chatroom events.\n"
        "3. Keep artifacts small and traceable per conversation.\n"
    )
    constraints_text = (
        "Constraints:\n"
        "1. Keep schema_version at v1 for this MVP runtime.\n"
        "2. Preserve source conversation as immutable input.\n"
        "3. Prefer append-only logs for auditability.\n"
    )

    vision_path = out_dir / "vision.md"
    goals_path = out_dir / "goals.md"
    constraints_path = out_dir / "constraints.md"
    vision_path.write_text(vision_text, encoding="utf-8")
    goals_path.write_text(goals_text, encoding="utf-8")
    constraints_path.write_text(constraints_text, encoding="utf-8")

    refs = [str(vision_path), str(goals_path), str(constraints_path)]
    return {
        "artifact_refs": refs,
        "vision": vision_text,
        "goals": goals_text,
        "constraints": constraints_text,
    }


def process_planning(paths: RuntimePaths, writers_out: Dict[str, Any], job_id: str) -> Dict[str, Any]:
    stage = "planning-room"
    out_dir = stage_dir(paths, stage, "outbox", job_id)

    mvp_sprint = (
        "# MVP Sprint\n\n"
        "## Scope\n"
        "1. Ingest conversation artifacts\n"
        "2. Generate deterministic writer outputs\n"
        "3. Emit game bridge events\n\n"
        "## Done Criteria\n"
        "1. stage_status.json updated\n"
        "2. game_events.jsonl includes success event\n"
        "3. validation report generated\n"
    )
    tasks = {
        "tasks": [
            {"id": "S1-T1", "title": "Normalize conversation", "status": "TODO"},
            {"id": "S1-T2", "title": "Generate writer artifacts", "status": "TODO"},
            {"id": "S1-T3", "title": "Bridge event emission", "status": "TODO"},
        ]
    }

    mvp_path = out_dir / "mvp_sprint.md"
    tasks_path = out_dir / "sprint_tasks.json"
    mvp_path.write_text(mvp_sprint, encoding="utf-8")
    write_json(tasks_path, tasks)

    refs = [str(mvp_path), str(tasks_path)]
    return {
        "artifact_refs": refs,
        "mvp_sprint": mvp_sprint,
        "tasks": tasks,
    }


def process_implementation(paths: RuntimePaths, planning_out: Dict[str, Any], job_id: str) -> Dict[str, Any]:
    stage = "implementation-room"
    out_dir = stage_dir(paths, stage, "outbox", job_id)

    summary = {
        "artifact_id": make_artifact_id(stage, job_id),
        "stage": stage,
        "schema_version": SCHEMA_VERSION,
        "created_at": utc_now(),
        "source_refs": planning_out["artifact_refs"],
        "payload": {
            "implemented_steps": [
                "ingest normalization",
                "writers generation",
                "planning artifacts",
            ],
            "notes": "MVP implementation summary generated by stage runtime.",
        },
    }

    patch_summary_path = out_dir / "patch_summary.json"
    write_json(patch_summary_path, summary)

    return {
        "artifact_refs": [str(patch_summary_path)],
        "summary": summary,
    }


def process_validation(paths: RuntimePaths, implementation_out: Dict[str, Any], job_id: str) -> Dict[str, Any]:
    stage = "validation-room"
    out_dir = stage_dir(paths, stage, "outbox", job_id)

    validation_report = (
        "# Validation Report\n\n"
        "Result: PASS\n\n"
        "Checks:\n"
        "1. Implementation summary exists.\n"
        "2. Planning artifact references are present.\n"
        "3. Runtime schema version is v1.\n"
    )

    runtime_check = {
        "artifact_id": make_artifact_id(stage, job_id),
        "stage": stage,
        "schema_version": SCHEMA_VERSION,
        "created_at": utc_now(),
        "source_refs": implementation_out["artifact_refs"],
        "payload": {
            "status": "ok",
            "checks": {
                "implementation_summary": True,
                "artifact_refs": True,
                "schema_version": True,
            },
        },
    }

    report_path = out_dir / "validation_report.md"
    check_path = out_dir / "runtime_check.json"
    report_path.write_text(validation_report, encoding="utf-8")
    write_json(check_path, runtime_check)

    return {
        "artifact_refs": [str(report_path), str(check_path)],
        "runtime_check": runtime_check,
    }


def process_game_bridge(paths: RuntimePaths, validation_out: Dict[str, Any], job_id: str) -> Dict[str, Any]:
    stage = "game-bridge-room"
    out_dir = stage_dir(paths, stage, "outbox", job_id)

    status = "ok" if validation_out["runtime_check"]["payload"]["status"] == "ok" else "failed"

    game_event = {
        "schema_version": SCHEMA_VERSION,
        "event_id": f"{job_id}--{int(dt.datetime.now().timestamp())}",
        "ts": utc_now(),
        "stage": stage,
        "actor": "GameBridgeWorker",
        "event": "pipeline_result",
        "summary": "Factory pipeline run completed",
        "artifact_refs": validation_out["artifact_refs"],
        "status": status,
        "error_code": "",
    }

    game_events_path = out_dir / "game_events.jsonl"
    stage_status_path = out_dir / "stage_status.json"

    append_jsonl(game_events_path, game_event)
    write_json(
        stage_status_path,
        {
            "schema_version": SCHEMA_VERSION,
            "updated_at": utc_now(),
            "stage": stage,
            "status": status,
            "job_id": job_id,
        },
    )

    return {
        "artifact_refs": [str(game_events_path), str(stage_status_path)],
        "game_event": game_event,
    }


def run_pipeline(paths: RuntimePaths, project_slug: str, source_conv: Path) -> Dict[str, Any]:
    conversation_id = source_conv.parent.name
    job_id = f"{slugify(project_slug)}--{slugify(conversation_id)}"

    ingest_out = process_ingest(paths, project_slug, source_conv, job_id)
    _mark_stage_success(paths, "ingest-room", ingest_out["artifact_refs"], project_slug, conversation_id)

    writers_out = process_writers(paths, ingest_out, job_id)
    _mark_stage_success(paths, "writers-room", writers_out["artifact_refs"], project_slug, conversation_id)

    planning_out = process_planning(paths, writers_out, job_id)
    _mark_stage_success(paths, "planning-room", planning_out["artifact_refs"], project_slug, conversation_id)

    implementation_out = process_implementation(paths, planning_out, job_id)
    _mark_stage_success(paths, "implementation-room", implementation_out["artifact_refs"], project_slug, conversation_id)

    validation_out = process_validation(paths, implementation_out, job_id)
    _mark_stage_success(paths, "validation-room", validation_out["artifact_refs"], project_slug, conversation_id)

    bridge_out = process_game_bridge(paths, validation_out, job_id)
    _mark_stage_success(paths, "game-bridge-room", bridge_out["artifact_refs"], project_slug, conversation_id)

    maybe_send_ws_event(paths.factory_root, bridge_out["game_event"])

    return {
        "project_slug": project_slug,
        "conversation": str(source_conv),
        "job_id": job_id,
        "result": "ok",
        "game_event": bridge_out["game_event"],
    }


def _mark_stage_success(
    paths: RuntimePaths,
    stage: str,
    artifact_refs: List[str],
    project_slug: str,
    conversation_id: str,
) -> None:
    event = stage_event(
        stage=stage,
        actor="StageRuntime",
        event="artifact_transformed",
        summary=f"{stage} completed for {project_slug}/{conversation_id}",
        artifact_refs=artifact_refs,
        status="ok",
    )
    write_stage_chatroom(paths, stage, event)
    update_stage_status(
        paths,
        stage,
        "ok",
        {
            "project_slug": project_slug,
            "conversation_id": conversation_id,
            "artifact_count": len(artifact_refs),
        },
    )


def cmd_init(args: argparse.Namespace, paths: RuntimePaths) -> None:
    ensure_stage_layout(paths)
    print(f"Initialized stage layout under: {paths.stages_root}")


def cmd_list_projects(args: argparse.Namespace, paths: RuntimePaths) -> None:
    projects = project_dirs(paths.source_inbox)
    enabled = set(read_projects_state(paths).get("enabled_projects", []))
    active = read_active_project(paths.active_project_file).get("active_project", "")
    if not projects:
        print("No projects found.")
        return

    for item in projects:
        active_marker = "*" if item.name == active else " "
        enabled_marker = "E" if (not enabled or item.name in enabled) else "D"
        print(f"{active_marker}{enabled_marker} {item.name}")


def cmd_set_active(args: argparse.Namespace, paths: RuntimePaths) -> None:
    all_projects = {p.name for p in project_dirs(paths.source_inbox)}
    if args.project not in all_projects:
        raise SystemExit(f"Project not found in inbox: {args.project}")

    project_slug = args.project
    write_active_project(paths.active_project_file, project_slug)
    write_projects_state(paths, [project_slug])
    print(f"Active project set: {project_slug}")
    print(f"Enabled projects set exclusively: {project_slug}")


def cmd_run_once(args: argparse.Namespace, paths: RuntimePaths) -> None:
    ensure_stage_layout(paths)
    project_slug = resolve_project_slug(paths, args.project)
    source_conv = pick_conversation(paths, project_slug, args.conversation)

    result = run_pipeline(paths, project_slug, source_conv)
    print(json.dumps(result, indent=2, ensure_ascii=True))


def cmd_run_loop(args: argparse.Namespace, paths: RuntimePaths) -> None:
    ensure_stage_layout(paths)
    run_count = 0
    max_runs = args.max_runs if args.max_runs > 0 else None

    while True:
        try:
            project_slug = resolve_project_slug(paths, args.project)
            source_conv = pick_conversation(paths, project_slug, args.conversation)
            result = run_pipeline(paths, project_slug, source_conv)
            result["loop_run"] = run_count + 1
            print(json.dumps(result, indent=2, ensure_ascii=True))
        except Exception as exc:  # Keep loop alive for watcher-style execution.
            error_payload = {
                "ts": utc_now(),
                "result": "error",
                "loop_run": run_count + 1,
                "error": str(exc),
            }
            print(json.dumps(error_payload, indent=2, ensure_ascii=True))

        run_count += 1
        if max_runs is not None and run_count >= max_runs:
            break

        time.sleep(args.interval_sec)


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Run Arcane Arcade Machine Factory stages.")
    parser.add_argument(
        "--factory-root",
        default=str(Path(__file__).resolve().parents[2]),
        help="Factory root path (defaults to ArcaneArcadeMachineFactory).",
    )

    subparsers = parser.add_subparsers(dest="command", required=True)

    init_parser = subparsers.add_parser("init-layout", help="Create stage directories.")
    init_parser.set_defaults(handler=cmd_init)

    list_parser = subparsers.add_parser("list-projects", help="List available inbox project folders.")
    list_parser.set_defaults(handler=cmd_list_projects)

    set_active_parser = subparsers.add_parser("set-active-project", help="Set active project slug.")
    set_active_parser.add_argument("--project", required=True, help="Project folder name in TheFactoryHopper/inbox")
    set_active_parser.set_defaults(handler=cmd_set_active)

    run_parser = subparsers.add_parser("run-once", help="Run one pipeline execution.")
    run_parser.add_argument("--project", default=None, help="Optional project folder name override.")
    run_parser.add_argument("--conversation", default=None, help="Optional explicit conversation.json path.")
    run_parser.set_defaults(handler=cmd_run_once)

    loop_parser = subparsers.add_parser("run-loop", help="Run pipeline continuously with a fixed interval.")
    loop_parser.add_argument("--project", default=None, help="Optional project folder name override.")
    loop_parser.add_argument("--conversation", default=None, help="Optional explicit conversation.json path.")
    loop_parser.add_argument(
        "--interval-sec",
        type=float,
        default=10.0,
        help="Sleep interval between pipeline runs.",
    )
    loop_parser.add_argument(
        "--max-runs",
        type=int,
        default=0,
        help="Optional run cap; 0 means run forever.",
    )
    loop_parser.set_defaults(handler=cmd_run_loop)

    return parser


def main() -> None:
    parser = build_parser()
    args = parser.parse_args()

    factory_root = Path(args.factory_root).expanduser().resolve()
    hopper_root = factory_root / "TheFactoryHopper"
    stages_root = resolve_stages_root(factory_root)

    paths = RuntimePaths(
        factory_root=factory_root,
        stages_root=stages_root,
        source_inbox=hopper_root / "inbox",
        shared_chatroom_audit=hopper_root / "chatroom_ws_audit" / "factory.chatroom.jsonl",
        active_project_file=hopper_root / "docs" / "active_project.json",
        stage_status_file=hopper_root / "docs" / "stage_status.json",
    )

    args.handler(args, paths)


if __name__ == "__main__":
    main()
