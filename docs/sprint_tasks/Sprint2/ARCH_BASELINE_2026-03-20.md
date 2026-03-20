# Sprint2 Architecture Baseline (2026-03-20)

Sprint: Sprint2 - Artifact Pipeline Core
Task: 1 - Architecture And Interface Baseline

## Component Diagram (text)

```
External Conversation Sources
        |
        v
 [TheFactoryHopper/inbox]      <- raw conversation.json artifacts land here
        |
        v
 [stage_runtime.py]            <- filesystem watcher, job claim, routing
        |
        v
 [stage_workers.py]            <- per-stage transform functions
    |         |       |
    v         v       v
process_    process_  process_
factory_    writers   sprint_
hopper               planning
    |         |       |
    v         v       v
 [outbox artifacts]            <- normalized conversation, vision, sprint tasks
        |
        v
 [arcane_event_contract.py]    <- validates/builds event envelope before publish
        |
        v
 [arcane_event_publishers.py]  <- FileEventPublisher appends to events.jsonl
        |
        v
 [arcane_event_projections.py] <- rebuilds read-model state from events.jsonl
        |
        v
 [stage_status.json]           <- snapshot consumed by Godot and monitoring
        |
        v
 [bridge/ws_broker.py]         <- re-broadcasts events to WebSocket channel
        |
        v
 [game/chatroom_test_scene.gd] <- Godot consumer renders conversation UI
```

## Layers

### 1. Ingestion Layer

Components:
- `TheFactoryHopper/inbox/` — drop zone for raw conversation artifacts.
- `stage_runtime.py` — walks inbox, claims one job at a time, dispatches to workers.
- `process_factory_hopper()` in `stage_workers.py` — normalizes raw conversation.json into hopper-canonical form.

Data flow:
- Input: `conversation.json` (arbitrary shape)
- Output: normalized `conversation.md` + `metadata.yaml` → outbox

Error semantics:
- Malformed input is moved to `failed/` with chatroom event `stage.work.failed`.
- Claim is idempotent via rename lock; duplicate claims are skipped.

### 2. Pipeline Transform Layer

Components:
- `process_writers()` — converts normalized conversation + metadata into vision, goals, personas, scope, risks, stories.
- `process_sprint_planning()` — converts product artifacts into engineering intent tasks.
- `process_observatory()` — expands context with research and patterns.

Data flow:
- Input: prior stage outbox artifacts
- Output: next stage inbox artifacts

Error semantics:
- Missing required input artifact raises `HandoffValidationError`.
- QA gate failure raises `QAGateValidationError` and is recorded to `debug/`.

### 3. Event Contract Layer

Components:
- `arcane_event_contract.py` — canonical envelope schema, validation, builder.
- `arcane_event_publishers.py` — `FileEventPublisher` (default), `TransportPlaceholderPublisher`.

Rollout controls:
- `emit_mode`: `legacy_only | shadow | contract_only`
- `projection_mode`: `legacy | shadow | enforce`
- Config persisted at `TheFactoryHopper/docs/event_rollout/config.json`.

### 4. Projection Layer

Components:
- `arcane_event_projections.py` — `build_projection_state()`, `write_projection_state()`, `load_projection_state()`.
- Input: `TheFactoryHopper/docs/event_stream/events.jsonl`
- Output: `TheFactoryHopper/docs/event_projections/state.json`

### 5. Bridge and Godot Layer

Components:
- `bridge/ws_broker.py` — WebSocket relay for live event delivery.
- `game/chatroom_test_scene.gd` — Godot consumer, state-first UI, message validation.

## Data Flow Summary

```
conversation.json
  -> factory-hopper (normalize)
  -> writers-room (product artifacts)
  -> sprint-planning-room (task artifacts)
  -> event stream (contract events appended throughout)
  -> projection (state.json rebuilt)
  -> stage_status.json (snapshot updated)
  -> ws_broker (live broadcast)
  -> Godot chatroom scene (UI rendered from conversation state)
```
