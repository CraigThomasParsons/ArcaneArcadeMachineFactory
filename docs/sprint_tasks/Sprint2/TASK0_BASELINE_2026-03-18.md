# Sprint2 Task 0 Baseline (2026-03-18)

Sprint: Sprint2 - Artifact Pipeline Core (Ingest -> Writers -> Planning)
Task: 0 - Scope Lock And Success Metrics
Status: Complete

## Scope Boundaries

### In Scope

1. Implement core pipeline flow for ingest-room, writers-room, and planning-room.
2. Normalize raw inbox artifacts into deterministic intermediate artifacts.
3. Produce planning-ready output artifacts for sprint task generation.
4. Preserve contract compatibility with Stage/Event foundations from Sprint1.
5. Keep observability visible through chatroom and debug artifacts.

### Out Of Scope

1. UI redesigns or major Godot feature work.
2. New event family redesign beyond compatibility-safe additions.
3. Multi-writer concurrency model beyond current single-writer assumptions.
4. Long-term retention policy implementation for all queue artifacts.

## Objective Metrics

1. Stage layout contract readiness:
- Target: ingest-room, writers-room, planning-room each include inbox/outbox/archive/failed/debug/chatroom.
- Baseline check: pass (`ingest-room=layout_ok`, `writers-room=layout_ok`, `planning-room=layout_ok`).

2. Foundation dependency readiness:
- Target: runtime + contract + bridge + game anchor files available.
- Baseline check: pass (`foundation_files=ok`).

3. Validation baseline readiness:
- Target: event contract and rollout validation artifacts present before Sprint2 implementation expansion.
- Baseline check: pass (`validation_baseline=ok`).

4. Stage inventory visibility:
- Target: documented stage inventory available for compatibility checks.
- Baseline check: pass (`12` stage directories discovered in ChatRooms/stages).

## Risk Register

1. Risk: Schema drift between pipeline outputs and downstream consumers.
- Owner: StageWorker/Contract owner.
- Mitigation: keep event envelope compatibility checks and explicit schema version notes in each task.

2. Risk: Queue artifact growth causing noisy operational state.
- Owner: Runtime operations owner.
- Mitigation: maintain ignore policy for runtime churn in git and define cleanup cadence in Sprint2 Task 4.

3. Risk: Non-deterministic transforms reduce reproducibility.
- Owner: Pipeline implementation owner.
- Mitigation: enforce deterministic ordering and add repeat-run checks in Sprint2 Task 4.

4. Risk: Planning outputs diverge from sprint task templates.
- Owner: Planning-room implementation owner.
- Mitigation: pin output format to docs/sprint_tasks conventions and validate shape before promotion.

## Assumptions

1. Sprint1 contract and rollout foundations remain the source of truth for event compatibility.
2. Single local environment execution is sufficient for Sprint2 implementation slices.
3. Existing stage directory topology remains stable during Sprint2.

## Entry Criteria For Task 1

1. Task 0 baseline document published.
2. Sprint2 tracker state moved from ready to in-progress.
3. Task 1 can proceed using this scope and metric baseline without ambiguity.
