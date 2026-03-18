# Arcane Arcade Machine Factory
## Stage Architecture

This document defines the stage topology that connects TheFactoryHopper and the Godot game layer.

## Goals

1. Make artifact flow deterministic.
2. Make stage outputs consumable by Godot.
3. Support replay and debugging from archived artifacts.
4. Keep stage contracts simple enough for agent workers.

## Topology Overview

1. ingest-room
2. writers-room
3. planning-room
4. implementation-room
5. validation-room
6. game-bridge-room

Flow:

conversation artifact -> ingest-room -> writers-room -> planning-room -> implementation-room -> validation-room -> game-bridge-room -> Godot runtime

## Stage Definitions

### ingest-room

Input:

1. conversation.json from TheFactoryHopper inbox.

Output:

1. normalized_conversation.json
2. intake_report.json

### writers-room

Input:

1. normalized_conversation.json

Output:

1. vision.md
2. goals.md
3. constraints.md

### planning-room

Input:

1. vision.md
2. goals.md
3. constraints.md

Output:

1. mvp_sprint.md
2. sprint task bundles

### implementation-room

Input:

1. sprint task bundles

Output:

1. implementation artifacts
2. patch summaries

### validation-room

Input:

1. implementation artifacts
2. validation scripts

Output:

1. validation_report.md
2. runtime_check.json

### game-bridge-room

Input:

1. validated artifacts
2. runtime_check.json

Output:

1. game_events.jsonl
2. stage_status.json

## Shared Stage Schema

Each artifact should include:

1. artifact_id
2. stage
3. schema_version
4. created_at
5. source_refs
6. payload

## Stage Completion Rules

A stage is considered complete when:

1. Output artifacts exist in outbox.
2. Chatroom event with status ok is appended.
3. Input is moved to archive.
4. Debug log includes execution duration and worker id.

## Failure Rules

1. Move input artifact to failed.
2. Emit chatroom event with status failed.
3. Emit machine-readable error_code.
4. Keep stack or diagnostic details in debug.

## Godot Consumption Plan

Godot runtime consumes two primary outputs from game-bridge-room:

1. game_events.jsonl for UI feed and in-game notifications.
2. stage_status.json for stage health, queue depth, and last-run metadata.

Godot should poll at fixed interval and process only unseen event ids.

## Current Baseline

1. Runtime validation script exists at /home/craigpar/Code/ArcaneArcadeMachineFactory/game/sprint2_task3_slice.gd.
2. Source inbox corpus exists at /home/craigpar/Code/ArcaneArcadeMachineFactory/TheFactoryHopper/inbox.
3. Stage protocol documents now define consistent contracts for agent execution.

## Next Build Steps

1. Create stage root directories for ingest-room through game-bridge-room.
2. Add first watcher worker for ingest-room.
3. Emit first synthetic game_events.jsonl stream.
4. Build Godot status panel against stage_status.json.
5. Validate full path from inbox artifact to game notification.
