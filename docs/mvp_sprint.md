# Arcane Arcade Machine Factory
## MVP Sprint Plan

Date: 2026-03-16

Objective:

Deliver an end-to-end path where factory artifacts generate game-visible events in Godot.

## MVP Success Criteria

1. New inbox artifact is processed through at least three stages.
2. game_events.jsonl is produced by game-bridge-room.
3. Godot displays stage status and latest chatroom events.
4. Validation suite remains green on every run.

## Sprint Sequence

### Sprint 1: Stage Foundations

1. Create stage directories and contracts.
2. Implement artifact schema validators.
3. Stand up chatroom append-only event format.

Exit criteria:

1. ingest-room through game-bridge-room structure exists and validates.

### Sprint 2: Pipeline Core

1. Implement ingest-room and writers-room workers.
2. Produce normalized conversation and vision/goals artifacts.
3. Add planning-room output for sprint tasks.

Exit criteria:

1. conversation.json to mvp_sprint/task artifacts runs end-to-end.

### Sprint 3: Godot Bridge

1. Implement game-bridge-room event emitter.
2. Add Godot ChatroomFeedLoader and StageStatusPanel.
3. Display latest stage events in-game.

Exit criteria:

1. Godot updates from factory outputs without manual file editing.

### Sprint 4: Validation And Hardening

1. Add failure-path tests for malformed artifacts.
2. Add lock strategy for write coordination.
3. Add checksum verification for artifact integrity.

Exit criteria:

1. Stable repeatable run with recovery guidance documented.

## Non-Goals For MVP

1. Full multiplayer features.
2. Cloud orchestration.
3. Complex UI skinning.

## Risks

1. Schema drift across stage workers.
2. File locking races under concurrent processing.
3. Godot polling lag for large event streams.

## Mitigations

1. Versioned schemas with explicit validators.
2. Single-writer policy until lock protocol is complete.
3. Event id checkpointing in Godot feed loader.
