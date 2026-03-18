# Stage Runtime

This folder is the runtime root for ArcaneArcadeMachineFactory stage workers.

## Stage Topology

1. factory-hopper
2. writers-room
3. observatory
4. sprint-planning-room
5. prompt-engineering-lab
6. blueprint-forge
7. factory-floor

Each stage uses this subdirectory contract:

1. agents
2. bin
3. docs
4. inbox
5. outbox
6. archive
7. failed
8. debug
9. chatroom

Each stage has a primary agent scaffold in:

1. ChatRooms/stages/<stage>/agents/<agent-slug>/README.md
2. ChatRooms/stages/<stage>/agents/<agent-slug>/agent.py

The `agent.py` hook can add stage-specific custom code via:

def transform(job, package_dir, out_dir) -> list[str]

## Runtime Script

Use the stage runtime script:

python TheFactoryHopper/bin/stage_runtime.py <command>

Commands:

1. init-layout
2. list-projects
3. set-active-project --project <project-folder-name>
4. run-once [--project <project-folder-name>] [--conversation <path/to/conversation.json>]
5. run-loop [--project <project-folder-name>] [--conversation <path/to/conversation.json>] [--interval-sec <seconds>] [--max-runs <n>]

## Queue Workers

Use the queue-driven worker runtime for independent stage workers:

python TheFactoryHopper/bin/stage_workers.py <command>

Commands:

1. init-layout
2. enqueue-conversation [--project <project-folder-name>] [--conversation <path/to/conversation.json>]
3. worker --stage <stage-name> --once
4. worker --stage <stage-name> [--interval-sec <seconds>]
5. worker-all-once
6. register-postal-profiles [--registry <path>]
7. next-prompt [--project <project-folder-name>] [--conversation <path/to/conversation.json>] [--role coder|qa|architect]

## Package Contract

Jobs are package folders, not loose queue files.

Each package contains:

1. next_job.json
2. letter.toml (for handoff stages)
3. manifest.toml (for handoff stages)
4. stage artifacts

Workers consume from `ChatRooms/stages/<stage>/inbox/<job-id>/next_job.json` and emit to `ChatRooms/stages/<stage>/outbox/<job-id>/`.
Handoff to the next stage is signaled through ThePostalService using `postal.signals`.
If the postal worker is unavailable, the runtime falls back to direct local move to keep the pipeline deterministic.

## PostalService Setup

Write Arcane stage profiles into ThePostalService registry:

python TheFactoryHopper/bin/stage_workers.py register-postal-profiles

## Interactive Control Deck

Run the local interactive panel server:

/home/craigpar/Code/ArcaneArcadeMachineFactory/.venv/bin/python TheFactoryHopper/bin/stage_panel_server.py --host 127.0.0.1 --port 8790

Then open:

http://127.0.0.1:8790

Example staged run:

1. python TheFactoryHopper/bin/stage_workers.py enqueue-conversation
2. python TheFactoryHopper/bin/stage_workers.py worker --stage factory-hopper --once
3. python TheFactoryHopper/bin/stage_workers.py worker --stage writers-room --once
4. python TheFactoryHopper/bin/stage_workers.py worker --stage observatory --once
5. python TheFactoryHopper/bin/stage_workers.py worker --stage sprint-planning-room --once
6. python TheFactoryHopper/bin/stage_workers.py worker --stage prompt-engineering-lab --once
7. python TheFactoryHopper/bin/stage_workers.py worker --stage blueprint-forge --once
8. python TheFactoryHopper/bin/stage_workers.py worker --stage factory-floor --once

## Typical Flow

1. Initialize stage folders.
2. Pick one active project.
3. Run one pipeline pass.
4. Review generated artifacts in each stage outbox.
5. Review status and chatroom logs.

## Outputs To Watch

1. TheFactoryHopper/docs/stage_status.json
2. TheFactoryHopper/chatroom_ws_audit/factory.chatroom.jsonl
3. ChatRooms/stages/factory-floor/outbox/<job-id>/generated_sprint/sprint_goal.md
4. ChatRooms/stages/factory-floor/outbox/<job-id>/generated_sprint/effort-1-micro-tasks.json
5. ChatRooms/stages/factory-floor/outbox/<job-id>/generated_sprint/0-tasks.md
6. Godot test scene Stage snapshot label reads from TheFactoryHopper/docs/stage_status.json
