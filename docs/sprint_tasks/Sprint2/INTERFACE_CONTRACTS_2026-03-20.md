# Sprint2 Interface Contracts (2026-03-20)

Sprint: Sprint2 - Artifact Pipeline Core
Task: 1 - Architecture And Interface Baseline

## 1. Event Envelope Contract

Module: `TheFactoryHopper/bin/arcane_event_contract.py`
Schema version: `v1` (`EVENT_SCHEMA_VERSION`)

### Required Fields

| Field            | Type   | Description                                             |
|------------------|--------|---------------------------------------------------------|
| `event_id`       | str    | UUID4, unique per event                                 |
| `event_type`     | str    | Must be one of `REQUIRED_EVENT_TYPES`                   |
| `ts_utc`         | str    | ISO-8601 UTC timestamp                                  |
| `correlation_id` | str    | UUID4, groups causally related events                   |
| `causation_id`   | str    | UUID4 of the parent event that triggered this one       |
| `aggregate_type` | str    | Domain object type (e.g. `"stage"`, `"task"`)           |
| `aggregate_id`   | str    | Identity of the domain object                           |
| `stage`          | str    | Stage name that emitted this event                      |
| `worker`         | str    | Worker/actor identifier                                 |
| `attempt`        | int    | 1-based retry counter                                   |
| `status`         | str    | One of `ALLOWED_STATUSES`                               |
| `payload`        | dict   | Stage-specific data (no required sub-fields)            |
| `schema_version` | str    | Must equal `"v1"`                                       |
| `artifact_refs`  | list   | Paths to referenced artifacts (may be empty)            |

### Allowed Event Types

```
stage.claim.requested   stage.claim.granted
stage.work.started      stage.work.heartbeat
stage.work.completed    stage.work.failed
stage.work.skipped      task.state.changed
review.requested        review.completed
scrum.blocker.raised    scrum.blocker.cleared
scrum.user_action.required
worker.exhaustion.updated
sprint.health.updated
```

### Allowed Statuses

```
received  started  completed  failed  skipped  blocked  ok
```

### Error Semantics

- `EventSchemaValidationError(ValueError)` — raised by `validate_event_envelope()` for any contract violation.
- Callers must not swallow this exception; it must propagate to the job failure path.

### Builder

```python
build_event_envelope(
    event_type: str,
    *,
    aggregate_type: str,
    aggregate_id: str,
    stage: str,
    worker: str,
    status: str,
    payload: dict,
    artifact_refs: list[str] | None = None,
    correlation_id: str | None = None,   # auto-generated if omitted
    causation_id: str | None = None,     # defaults to event_id if omitted
    attempt: int = 1,
) -> dict
```

---

## 2. Event Publisher Contract

Module: `TheFactoryHopper/bin/arcane_event_publishers.py`

### Protocol

```python
class EventPublisher(Protocol):
    def publish(self, payload: dict) -> None: ...
    def describe(self) -> str: ...
```

### Implementations

| Class                        | Behaviour                                             |
|------------------------------|-------------------------------------------------------|
| `FileEventPublisher`         | Appends JSON line to `events.jsonl`; raises `EventPublishError(transient=False)` on I/O failure |
| `TransportPlaceholderPublisher` | Always raises `EventPublishError(transient=True)` — placeholder for future transport |

### Factory

```python
build_event_publisher(
    *,
    publisher_type: str,       # "file" | anything else → placeholder
    event_stream_path: Path,
) -> EventPublisher
```

### Error Semantics

- `EventPublishError(RuntimeError)` — has `.transient: bool`.
- Transient=True: retry may succeed; Transient=False: permanent failure, move job to `failed/`.

---

## 3. Projection Contract

Module: `TheFactoryHopper/bin/arcane_event_projections.py`
Schema version: `v1` (`PROJECTION_SCHEMA_VERSION`)

### Public Functions

```python
load_event_stream_events(
    event_stream_path: Path,
    *,
    start_line: int = 1,
    max_events: int = 0,        # 0 = unbounded
) -> list[dict]

build_projection_state_from_events(
    events: list[dict],
    *,
    source_stream: Path,
    source_filters: dict,
) -> dict

build_projection_state(event_stream_path: Path) -> dict

write_projection_state(output_path: Path, payload: dict) -> None

load_projection_state(path: Path) -> dict
```

### Output Shape (state.json)

Top-level keys: `schema_version`, `generated_at`, `source_stream`, `event_count`,
`blockers`, `tasks`, `workers`, `sprint_health`.

### Error Semantics

- Malformed JSON lines in stream are silently skipped (logged at line metadata level).
- Missing stream file returns empty list, not exception.
- `load_projection_state` returns `{}` on missing or unreadable file.

---

## 4. Stage Worker Contract

Module: `TheFactoryHopper/bin/stage_workers.py`

### WorkerPaths

```python
@dataclass
class WorkerPaths:
    factory_root: Path      # repo root
    stages_root: Path       # ChatRooms/stages
    inbox: Path             # TheFactoryHopper/inbox
    docs: Path              # TheFactoryHopper/docs
```

### Job Dict Shape

| Field           | Type | Required | Description                          |
|-----------------|------|----------|--------------------------------------|
| `job_id`        | str  | yes      | UUID4                                |
| `stage`         | str  | yes      | Stage name                           |
| `project_slug`  | str  | yes      | Project identifier                   |
| `created_at`    | str  | yes      | ISO-8601 UTC                         |
| `conversation_path` | str | yes | Path to source conversation file   |
| `payload`       | dict | yes      | Stage-specific fields                |

### Pipeline Functions

```python
process_factory_hopper(paths: WorkerPaths, job: dict) -> list[str]
process_writers(paths: WorkerPaths, job: dict) -> list[str]
process_observatory(paths: WorkerPaths, job: dict) -> list[str]
process_sprint_planning(paths: WorkerPaths, job: dict) -> list[str]
```

All return a list of output artifact paths written to `outbox/`.

### Error Semantics

| Exception                      | When                                     | Recovery                      |
|-------------------------------|------------------------------------------|-------------------------------|
| `HandoffValidationError`      | Required outbox artifact missing         | Job moved to `failed/`        |
| `QAGateValidationError`       | Contract event shape rejected            | Job moved to `failed/`        |
| `AssignmentPolicyValidationError` | Ownership constraint violation       | Job moved to `failed/`        |

---

## 5. Chatroom Event Contract

Consumed by: `bridge/ws_broker.py`, `game/chatroom_test_scene.gd`

### Required Fields

| Field          | Type   | Description                           |
|----------------|--------|---------------------------------------|
| `ts`           | str    | ISO-8601 UTC                          |
| `stage`        | str    | Stage name                            |
| `actor`        | str    | Worker or agent name                  |
| `event`        | str    | Event type label                      |
| `summary`      | str    | Human-readable one-line               |
| `artifact_refs`| list   | Paths of related artifacts            |
| `status`       | str    | `ok` or `failed`                      |
| `error_code`   | str    | Empty string on success               |

### Error Semantics

- Chatroom log is append-only; malformed entries must not overwrite history.
- Godot consumer silently skips entries missing required fields rather than crashing.

---

## 6. Godot Conversation State Contract

Module: `game/chatroom_test_scene.gd`

### In-memory entry shape

```gdscript
{
    "speaker": str,    # SPEAKER_USER | SPEAKER_PIPER | SPEAKER_MASON | ...
    "text": str,       # strip_edges(), max MAX_MESSAGE_LENGTH chars
    "ts": str,         # ISO-8601 from Time.get_datetime_string_from_system(true)
}
```

### Validation rules

- `speaker` must not be empty.
- `text` must not be empty after strip_edges().
- `text` longer than `MAX_MESSAGE_LENGTH` (1000) is rejected on send and truncated on load.
- `SPEAKER_SYSTEM` entries are never persisted to disk.
- History is capped at `MAX_CONVERSATION_ENTRIES` (1000); oldest entries are dropped when exceeded.
