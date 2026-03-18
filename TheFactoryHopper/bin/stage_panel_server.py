#!/usr/bin/env python3
"""Interactive control panel server for TheFactoryHopper.

This server is intentionally lightweight. It serves a static dashboard and a
small JSON API so operators can monitor and control stage workers in real time.
"""

from __future__ import annotations

import argparse
import json
import subprocess
import sys
import urllib.parse
from dataclasses import dataclass
from datetime import datetime, timezone
from http import HTTPStatus
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple


# The stage order is fixed so UI lanes are stable between refreshes.
# Stable ordering matters for operator muscle memory during busy runs.
# This mirrors the worker runtime topology exactly.
STAGE_NAMES: Tuple[str, ...] = (
    "factory-hopper",
    "writers-room",
    "observatory",
    "sprint-planning-room",
    "prompt-engineering-lab",
    "blueprint-forge",
)

# Jobs are package folders and must contain this file to be consumable.
# We keep this contract in one place so queue calculations are correct.
# Any future rename only needs one change.
PACKAGE_JOB_FILE = "next_job.json"


@dataclass
class PanelPaths:
    # Root path for TheFactoryHopper assets and runtime folders.
    # The server only reads/writes within this subtree for safety.
    # Worker command invocation is also anchored from this root.
    hopper_root: Path

    # Stage folders hold inbox/outbox/archive/failed and per-stage events.
    # The overview endpoint reads these to build lane status quickly.
    # We avoid deep scans to keep UI refresh responsive.
    stages_root: Path

    # Shared audit log provides cross-stage timeline events.
    # The panel tails this file for the live event stream.
    # This keeps UX aligned with existing websocket audit data.
    shared_audit_file: Path

    # Stage status snapshot is already used by Godot and workers.
    # Reusing it avoids duplicate status computation code paths.
    # The panel enriches this with queue metrics from the filesystem.
    stage_status_file: Path

    # Static UI files are served from here.
    # Frontend JS uses same-origin API calls to avoid CORS complexity.
    # This keeps startup simple for local-first operations.
    ui_root: Path

    # Worker script is called for interactive control actions.
    # Actions are intentionally thin wrappers around existing CLI commands.
    # This avoids drifting logic between UI server and worker runtime.
    worker_script: Path


class PanelState:
    """Holds immutable server config and helper routines.

    We centralize file access and worker command execution here so request
    handlers stay short, predictable, and easier to reason about.
    """

    def __init__(self, paths: PanelPaths) -> None:
        # Paths are provided by CLI startup resolution.
        # This lets us run the server from any current working directory.
        # It also makes testing with alternate roots straightforward.
        self.paths = paths

    def _utc_now(self) -> str:
        # UTC timestamps simplify timeline ordering across components.
        # Workers and status snapshots already use UTC conventions.
        # Keeping the same format reduces parsing surprises.
        return datetime.now(tz=timezone.utc).isoformat()

    def _load_json(self, path: Path, fallback: Dict[str, Any]) -> Dict[str, Any]:
        # Missing files are normal during first-run bootstrap.
        # Returning a typed fallback keeps callers deterministic.
        # Corrupt JSON also falls back to avoid dashboard hard-failure.
        if not path.exists():
            return fallback
        try:
            return json.loads(path.read_text(encoding="utf-8"))
        except Exception:
            return fallback

    def _tail_jsonl(self, path: Path, limit: int) -> List[Dict[str, Any]]:
        # Tail behavior keeps refreshes fast even with long audit history.
        # We parse only the last N lines the operator is likely to inspect.
        # Invalid lines are skipped so one bad event does not break the UI.
        if not path.exists():
            return []
        lines = path.read_text(encoding="utf-8").splitlines()[-limit:]
        payloads: List[Dict[str, Any]] = []
        for line in lines:
            try:
                payloads.append(json.loads(line))
            except Exception:
                continue
        return payloads

    def _queue_depth(self, stage: str) -> int:
        # Queue depth should reflect package jobs that workers can consume.
        # We count valid package folders and include legacy json jobs.
        # Legacy inclusion helps during migration and replay windows.
        inbox = self.paths.stages_root / stage / "inbox"
        if not inbox.exists():
            return 0

        package_count = len([p for p in inbox.iterdir() if p.is_dir() and (p / PACKAGE_JOB_FILE).exists()])
        legacy_count = len(list(inbox.glob("*.json")))
        return package_count + legacy_count

    def _folder_count(self, stage: str, bucket: str) -> int:
        # Bucket counts provide fast visual signal on health and throughput.
        # We count direct children only because each child is one package id.
        # Missing folders are treated as empty for robustness.
        target = self.paths.stages_root / stage / bucket
        if not target.exists():
            return 0
        return len([p for p in target.iterdir()])

    def _last_stage_event(self, stage: str) -> Dict[str, Any]:
        # Per-stage last event gives operators immediate context.
        # We do not scan the shared audit here to avoid cross-stage ambiguity.
        # Empty value shape stays consistent for frontend rendering.
        events_file = self.paths.stages_root / stage / "chatroom" / "events.jsonl"
        items = self._tail_jsonl(events_file, 1)
        return items[-1] if items else {}

    def build_overview(self) -> Dict[str, Any]:
        # Snapshot status file stores semantic stage status from workers.
        # We enrich this with live filesystem queue metrics for interactivity.
        # This merged view is the primary payload for the dashboard lanes.
        status_doc = self._load_json(self.paths.stage_status_file, {"schema_version": "v1", "stages": {}})
        stages_doc: Dict[str, Any] = status_doc.get("stages", {})

        stages: List[Dict[str, Any]] = []
        for stage in STAGE_NAMES:
            stage_status = stages_doc.get(stage, {})
            stages.append(
                {
                    "name": stage,
                    "queue_depth": self._queue_depth(stage),
                    "outbox_count": self._folder_count(stage, "outbox"),
                    "archive_count": self._folder_count(stage, "archive"),
                    "failed_count": self._folder_count(stage, "failed"),
                    "status": stage_status.get("status", "unknown"),
                    "status_updated_at": stage_status.get("updated_at", ""),
                    "status_details": stage_status.get("details", {}),
                    "last_event": self._last_stage_event(stage),
                }
            )

        # Shared event stream helps operators correlate actions across lanes.
        # We keep the limit modest for UI readability and response size.
        # Polling every few seconds stays cheap with this bounded payload.
        events = self._tail_jsonl(self.paths.shared_audit_file, 60)
        return {
            "ts": self._utc_now(),
            "stages": stages,
            "events": events,
        }

    def run_worker_action(self, action: str, stage: Optional[str]) -> Dict[str, Any]:
        # Actions are constrained to known operations to avoid command abuse.
        # We shell out to stage_workers so all behavior stays centralized.
        # Output is returned raw and parsed when possible for UI detail.
        base_cmd = [sys.executable, str(self.paths.worker_script)]

        if action == "worker_all_once":
            cmd = base_cmd + ["worker-all-once"]
        elif action == "run_once":
            if not stage:
                raise ValueError("stage is required for run_once")
            if stage not in STAGE_NAMES:
                raise ValueError(f"unknown stage: {stage}")
            cmd = base_cmd + ["worker", "--stage", stage, "--once"]
        else:
            raise ValueError(f"unsupported action: {action}")

        proc = subprocess.run(
            cmd,
            cwd=str(self.paths.hopper_root.parent),
            capture_output=True,
            text=True,
            check=False,
        )

        # Worker outputs are JSON by convention, but we keep raw output too.
        # This helps debugging when exceptions produce plain-text traces.
        # Exit code is always included so operators can detect failure quickly.
        parsed: Any = None
        raw = (proc.stdout or "") + (proc.stderr or "")
        try:
            parsed = json.loads(proc.stdout)
        except Exception:
            parsed = None

        return {
            "exit_code": proc.returncode,
            "output": raw.strip(),
            "parsed": parsed,
            "command": cmd,
        }


class PanelHandler(BaseHTTPRequestHandler):
    """Request handler for static files and control API."""

    server_version = "StagePanel/0.1"

    @property
    def state(self) -> PanelState:
        # The HTTP server stores shared state in a typed attribute.
        # Access through a property keeps handler methods concise.
        # This cast is safe because setup happens at startup.
        return self.server.panel_state  # type: ignore[attr-defined]

    def _send_json(self, payload: Dict[str, Any], status: int = HTTPStatus.OK) -> None:
        # Consistent JSON responses simplify frontend fetch logic.
        # We always send UTF-8 with explicit content length.
        # No-cache headers keep polling views fresh.
        blob = json.dumps(payload, indent=2, ensure_ascii=True).encode("utf-8")
        self.send_response(status)
        self.send_header("Content-Type", "application/json; charset=utf-8")
        self.send_header("Cache-Control", "no-store")
        self.send_header("Content-Length", str(len(blob)))
        self.end_headers()
        self.wfile.write(blob)

    def _send_file(self, path: Path, content_type: str) -> None:
        # Static file serving is intentionally minimal and local-only.
        # Missing files return 404 so UI issues are obvious.
        # Binary-safe read allows future asset expansion.
        if not path.exists() or not path.is_file():
            self.send_error(HTTPStatus.NOT_FOUND, "Not found")
            return
        data = path.read_bytes()
        self.send_response(HTTPStatus.OK)
        self.send_header("Content-Type", content_type)
        self.send_header("Cache-Control", "no-store")
        self.send_header("Content-Length", str(len(data)))
        self.end_headers()
        self.wfile.write(data)

    def do_GET(self) -> None:
        # Parse path once so routing branches are deterministic.
        # Query values are used for event limits on demand.
        # Unknown routes fail closed with 404.
        parsed = urllib.parse.urlparse(self.path)
        route = parsed.path

        if route == "/" or route == "/index.html":
            return self._send_file(self.state.paths.ui_root / "index.html", "text/html; charset=utf-8")
        if route == "/app.js":
            return self._send_file(self.state.paths.ui_root / "app.js", "text/javascript; charset=utf-8")
        if route == "/styles.css":
            return self._send_file(self.state.paths.ui_root / "styles.css", "text/css; charset=utf-8")
        if route == "/api/overview":
            return self._send_json(self.state.build_overview())
        if route == "/api/events":
            params = urllib.parse.parse_qs(parsed.query)
            limit = int(params.get("limit", ["60"])[0])
            return self._send_json({"events": self.state._tail_jsonl(self.state.paths.shared_audit_file, max(1, min(limit, 300)))})

        self.send_error(HTTPStatus.NOT_FOUND, "Not found")

    def do_POST(self) -> None:
        # POST is limited to explicit control actions.
        # Body is parsed as JSON and validated before execution.
        # Action responses include command output for operator feedback.
        parsed = urllib.parse.urlparse(self.path)
        if parsed.path != "/api/action":
            self.send_error(HTTPStatus.NOT_FOUND, "Not found")
            return

        content_len = int(self.headers.get("Content-Length", "0"))
        raw = self.rfile.read(content_len) if content_len > 0 else b"{}"
        try:
            payload = json.loads(raw.decode("utf-8"))
        except Exception:
            return self._send_json({"error": "invalid JSON payload"}, HTTPStatus.BAD_REQUEST)

        action = str(payload.get("action", "")).strip()
        stage = payload.get("stage")
        stage_name = str(stage) if stage is not None else None

        try:
            result = self.state.run_worker_action(action=action, stage=stage_name)
        except ValueError as exc:
            return self._send_json({"error": str(exc)}, HTTPStatus.BAD_REQUEST)

        status = HTTPStatus.OK if result["exit_code"] == 0 else HTTPStatus.BAD_REQUEST
        self._send_json({"result": result}, status)


def build_paths(factory_root: Path) -> PanelPaths:
    # Path construction is centralized so startup behavior is easy to inspect.
    # We keep all references relative to factory_root for portability.
    # This also avoids accidental cross-repo file reads.
    hopper_root = factory_root / "TheFactoryHopper"
    stages_root = factory_root / "ChatRooms" / "stages"
    return PanelPaths(
        hopper_root=hopper_root,
        stages_root=stages_root,
        shared_audit_file=hopper_root / "chatroom_ws_audit" / "factory.chatroom.jsonl",
        stage_status_file=hopper_root / "docs" / "stage_status.json",
        ui_root=hopper_root / "ui" / "control_panel",
        worker_script=hopper_root / "bin" / "stage_workers.py",
    )


def build_parser() -> argparse.ArgumentParser:
    # CLI entry allows operators to pick host/port explicitly.
    # Factory root defaults to current repository layout.
    # This keeps launch friction low for local usage.
    parser = argparse.ArgumentParser(description="Interactive control panel for TheFactoryHopper stage workers.")
    parser.add_argument(
        "--factory-root",
        default=str(Path(__file__).resolve().parents[2]),
        help="Factory root path (defaults to ArcaneArcadeMachineFactory).",
    )
    parser.add_argument("--host", default="127.0.0.1", help="Host interface to bind.")
    parser.add_argument("--port", type=int, default=8790, help="Port to bind.")
    return parser


def main() -> None:
    # Startup validates expected files early for clearer operator errors.
    # We run a threaded server so polling and action calls can overlap.
    # The server remains local-first and intentionally simple.
    parser = build_parser()
    args = parser.parse_args()

    factory_root = Path(args.factory_root).expanduser().resolve()
    paths = build_paths(factory_root)

    if not paths.ui_root.exists():
        raise SystemExit(f"UI root not found: {paths.ui_root}")
    if not paths.worker_script.exists():
        raise SystemExit(f"Worker script not found: {paths.worker_script}")

    panel_state = PanelState(paths)
    server = ThreadingHTTPServer((args.host, args.port), PanelHandler)
    server.panel_state = panel_state  # type: ignore[attr-defined]

    print(json.dumps({"result": "ok", "url": f"http://{args.host}:{args.port}", "ts": datetime.now(timezone.utc).isoformat()}, indent=2))
    server.serve_forever()


if __name__ == "__main__":
    main()
