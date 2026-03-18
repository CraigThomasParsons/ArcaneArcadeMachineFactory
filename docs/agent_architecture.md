# Arcane Arcade Machine Factory
## Agent Architecture

This document defines the runtime architecture for agents that connect the game layer and the factory layer.

## Intent

The system has two truths that must stay synchronized:

1. The game world in Godot.
2. The artifact pipeline in TheFactoryHopper.

Agents are responsible for translating between these worlds in a predictable, auditable way.

## Runtime Layers

### 1) Orchestrator Layer

Purpose:

1. Watch stage inbox folders.
2. Claim work safely.
3. Route jobs to the correct worker.
4. Persist logs and status.

Implementation options:

1. Rust worker binary for high-reliability daemon mode.
2. Python worker script for fast iteration mode.

### 2) Brain Layer

Purpose:

1. Parse input artifacts.
2. Produce transformed artifacts.
3. Emit structured reasoning notes for chatroom logs.

Implementation language:

1. Python by default for LLM integrations.

### 3) Artifact Layer

Purpose:

1. Provide deterministic file-based interfaces.
2. Preserve all stage inputs and outputs.
3. Make failures visible and recoverable.

Core directories per stage:

1. inbox
2. outbox
3. archive
4. failed
5. debug
6. chatroom

### 4) Godot Integration Layer

Purpose:

1. Render stage and chatroom state in-game.
2. Post game events into stage inboxes.
3. Poll and display stage outputs.

Current baseline:

1. Prototype runtime script exists at /home/craigpar/Code/ArcaneArcadeMachineFactory/game/sprint2_task3_slice.gd.
2. Stage input corpus exists under /home/craigpar/Code/ArcaneArcadeMachineFactory/TheFactoryHopper/inbox.

## Agent Types

### Stage Worker Agent

Consumes one artifact type and emits one artifact type.

Contract:

1. Input path
2. Output path
3. Stage id
4. Job id

### Chatroom Logger Agent

Appends normalized stage events to chatroom logs.

Contract:

1. stage
2. event_type
3. actor
4. summary
5. artifact_refs

### Bridge Agent

Translates between game intents and stage jobs.

Contract:

1. Game event to artifact request.
2. Stage output to game notification.

## Job Lifecycle

1. Detect new inbox artifact.
2. Claim by rename or lock.
3. Validate input schema.
4. Execute transform.
5. Write output artifact.
6. Append chatroom entry.
7. Archive input and write debug metadata.

Failure branch:

1. Move artifact to failed.
2. Append failure chatroom entry.
3. Emit machine-readable error code.

## Integration Contracts

### Artifact Contract

Required metadata for each artifact:

1. job_id
2. stage
3. created_at
4. source_artifact
5. schema_version

### Chatroom Contract

Required metadata for each chat message:

1. timestamp
2. stage
3. actor
4. event
5. references

### Godot Bridge Contract

Required fields for game-facing event packet:

1. event_id
2. stage
3. status
4. headline
5. artifact_path

## Reliability Requirements

1. Idempotent job processing.
2. Deterministic file naming conventions.
3. Recoverability via archive, failed, and debug folders.
4. Versioned schemas for artifacts and chatroom entries.

## Near-Term Implementation Plan

1. Add stage watcher worker for TheFactoryHopper inbox.
2. Define first game-facing stage event schema.
3. Build Godot polling node for stage outputs.
4. Add chatroom UI panel in Godot that tails stage logs.
5. Add replay mode that rebuilds game state from archived stage events.
