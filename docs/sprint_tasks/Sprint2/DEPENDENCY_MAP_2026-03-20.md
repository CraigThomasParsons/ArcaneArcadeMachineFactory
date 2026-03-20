# Sprint2 Dependency Map (2026-03-20)

Sprint: Sprint2 - Artifact Pipeline Core
Task: 1 - Architecture And Interface Baseline

## Module Dependency Graph

```
TheFactoryHopper/bin/
├── arcane_event_contract.py
│   └── (stdlib only: uuid, datetime, typing)
│
├── arcane_event_publishers.py
│   └── depends on: arcane_event_contract.py (EventSchemaValidationError)
│
├── arcane_event_projections.py
│   └── depends on: arcane_event_contract.py (PROJECTION_SCHEMA_VERSION)
│
├── stage_workers.py
│   ├── depends on: arcane_event_contract.py  (build_event_envelope, validate_event_envelope)
│   ├── depends on: arcane_event_publishers.py (build_event_publisher, EventPublishError)
│   └── depends on: arcane_event_projections.py (build_projection_state, write_projection_state)
│
└── stage_runtime.py
    ├── depends on: stage_workers.py (process_* functions, WorkerPaths, enqueue_job, consume_one_job, archive_job)
    └── depends on: arcane_event_projections.py (load_projection_state)

bridge/
└── ws_broker.py
    └── depends on: TheFactoryHopper/chatroom_ws_audit/factory.chatroom.jsonl (read-only tail)

game/
└── chatroom_test_scene.gd
    └── depends on: bridge/ws_broker.py (WebSocket, port 8765)
    └── depends on: stage_status.json (poll, read-only)
```

## External Dependency Inventory

| Package        | Used by                         | Purpose                         | Pinned? |
|----------------|---------------------------------|---------------------------------|---------|
| `websockets`   | bridge/ws_broker.py             | WebSocket server                | requirements.txt |
| `asyncio`      | bridge/ws_broker.py             | event loop                      | stdlib  |
| `jsonschema`   | (optional, arcane_event_contract future path) | schema validation | not yet pinned |
| GDScript 4.x   | game/*.gd                       | Godot runtime                   | Godot 4.x |

No PyPI packages are imported by any module in `TheFactoryHopper/bin/` — stdlib only.

## Ownership Map

| Component                     | Owned by          | Contract authority                             |
|-------------------------------|-------------------|------------------------------------------------|
| Event Envelope schema         | `arcane_event_contract.py` | StageWorker team — all changes require tests  |
| FileEventPublisher I/O format | `arcane_event_publishers.py` | StageWorker team                           |
| Projection state shape        | `arcane_event_projections.py` | StageWorker team                          |
| Job queue format (inbox JSON) | `stage_runtime.py` + `stage_workers.py` | Runtime ops              |
| Stage directory layout        | `ChatRooms/stages/<stage>/`  | Planning-room agent                         |
| Chatroom event log format     | `bridge/ws_event_schema.json` | Bridge team                              |
| Godot conversation state      | `game/chatroom_test_scene.gd` | Game prototype team                       |

## Interface Stability Classification

| Interface                         | Stability | Rationale                             |
|-----------------------------------|-----------|---------------------------------------|
| `validate_event_envelope()`       | Frozen    | All events must pass; breaking = data loss |
| `build_event_envelope()`          | Stable    | Additive changes only                 |
| `EventPublisher` protocol         | Stable    | New implementations must match protocol |
| `FileEventPublisher.publish()`    | Frozen    | Chat audit log depends on line format |
| `build_projection_state()`        | Evolving  | Shape may grow new top-level keys     |
| `process_factory_hopper()` return | Stable    | Returns artifact path list            |
| `process_writers()` return        | Stable    | Returns artifact path list            |
| Job dict shape                    | Evolving  | New optional fields may be added      |
| Chatroom event log fields         | Stable    | Godot UI reads all required fields    |
| Godot conversation entry shape    | Frozen    | Persisted to disk; backward compat required |

## Cross-Boundary Data Flow

```
Inbox JSON (job) ──► process_*(paths, job)
                         │
                         ├──► validate_event_envelope() — raises on violation
                         ├──► build_event_envelope()    — pure, no I/O
                         ├──► FileEventPublisher.publish() — appends events.jsonl
                         ├──► build_projection_state()  — reads events.jsonl
                         ├──► write_projection_state()  — writes stage_status.json
                         └──► returns [artifact_path, ...]
                                        │
                         ws_broker.py ◄─┘ tails chatroom.jsonl (separate file)
                                │
                         Godot (WebSocket) ◄── stage_status.json (poll)
```

## Shared Mutable Resources

| Resource                          | Writers                    | Readers                        | Conflict risk |
|-----------------------------------|----------------------------|--------------------------------|---------------|
| `TheFactoryHopper/chatroom_ws_audit/factory.chatroom.jsonl` | stage_workers.py | ws_broker.py | Low — append-only |
| `TheFactoryHopper/docs/projects_state.json` | stage_workers.py | stage_runtime.py | Medium — lock needed if concurrent |
| `TheFactoryHopper/docs/active_project.json` | stage_runtime.py | stage_workers.py | Low — single writer |
| Stage inbox dirs                  | external agents / runtime  | stage_workers.py               | Low — consume-once |

## Gap Register

| Gap | Risk | Owner |
|-----|------|-------|
| No locking around `projects_state.json` writes | Medium | Runtime ops |
| `TransportPlaceholderPublisher` always raises — no live transport | Medium | Bridge team |
| `jsonschema` not yet used in contract validation (manual key checks) | Low | StageWorker team |
| Godot WebSocket reconnect logic not implemented | Low | Game prototype team |
