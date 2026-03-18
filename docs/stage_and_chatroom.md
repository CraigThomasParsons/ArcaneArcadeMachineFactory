# Arcane Arcade Machine Factory
## Stage And Chatroom Protocol

This document defines how stages and chatrooms work together and how Godot consumes that state.

## Core Model

A stage transforms artifacts.

A chatroom records why and how those transformations happened.

Agents never coordinate by direct messaging. They coordinate through:

1. Structured artifacts.
2. Append-only chatroom events.

## Stage Directory Contract

Each stage uses the same filesystem layout:

1. inbox
2. outbox
3. archive
4. failed
5. debug
6. chatroom

Meaning:

1. inbox: queued input artifacts.
2. outbox: completed output artifacts.
3. archive: successful input snapshots.
4. failed: failed inputs with diagnostics.
5. debug: machine and operator logs.
6. chatroom: append-only human and agent-readable event log.

## Chatroom Purpose

Chatrooms are operational memory.

They answer:

1. What happened?
2. Why did it happen?
3. Which artifact changed?
4. What failed and how was it handled?

## Chatroom Event Format

All entries should be valid line-delimited JSON.

Required fields:

1. ts
2. stage
3. actor
4. event
5. summary
6. artifact_refs
7. status
8. error_code

Example event:

{"ts":"2026-03-16T15:21:00Z","stage":"writers-room","actor":"WriterAgent","event":"artifact_transformed","summary":"conversation to vision complete","artifact_refs":["inbox/conversation.json","outbox/vision.json"],"status":"ok","error_code":""}

## Stage Work Protocol

1. Claim job from inbox.
2. Validate schema and required metadata.
3. Execute transformation.
4. Emit output artifact.
5. Append success chatroom event.
6. Archive source artifact.

Failure path:

1. Move source artifact to failed.
2. Append failure chatroom event with error_code.
3. Write debug payload in debug.

## Godot Chatroom Integration

Godot should not write arbitrary prose to stage chatrooms. It should write structured events.

Live transport model:

1. Primary: local WebSocket broker for real-time delivery.
2. Secondary: JSONL chatroom log for audit and replay.

Godot integration responsibilities:

1. Subscribe to broker channel for live stage events.
2. Emit internal Godot signals for UI and gameplay consumers.
3. Trigger notifications when failed events arrive.
4. Allow artifact link navigation from event metadata.

Recommended game nodes:

1. ChatroomBridge node: WebSocket client that emits event signals.
2. StageStatusPanel node: aggregates stage health and queue counts.
3. ArtifactDetailPanel node: displays selected event metadata.

## Guardrails

1. Chatroom logs are append-only.
2. Do not mutate historical events.
3. All stage events must include artifact references.
4. Event schema changes require schema_version bump.

## Initial MVP Stages

1. factory-hopper: normalize conversation and metadata artifacts.
2. writers-room: generate lean inception and product artifacts.
3. observatory: expand context with research and patterns artifacts.
4. sprint-planning-room: convert product artifacts into engineering intent.
5. prompt-engineering-lab: generate role-specific coding prompts.
6. blueprint-forge: emit repository structure, module plan, and contracts.
7. game-bridge-room: expose game-ready events and directives.

## Why This Matters

This protocol makes the factory and game interoperable:

1. Factory stays deterministic and auditable.
2. Godot gets real-time pipeline visibility.
3. Agents keep shared context without hidden state.
