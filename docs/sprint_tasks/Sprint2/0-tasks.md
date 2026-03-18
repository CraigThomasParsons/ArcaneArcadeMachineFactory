# Task 0: Scope Lock And Success Metrics

## Goals

- lock the scope for Artifact Pipeline Core
- define measurable sprint outcomes

## Context

This sprint focuses on turn raw inbox artifacts into planning artifacts.

## Requirements

- explicit in-scope and out-of-scope list
- measurable success criteria
- risk register initialized

## Acceptance Criteria

- scope and metrics are documented and agreed
- top risks have mitigation owners

## Implementation Steps

1. Document scope boundaries.
2. Define objective metrics.
3. Capture top risks and mitigations.
4. Publish sprint baseline notes.

## Completion Notes (2026-03-18)

Task 0 completed for Sprint2.

### Published Baseline

- `docs/sprint_tasks/Sprint2/TASK0_BASELINE_2026-03-18.md`

### Scope Lock Summary

- In scope: ingest-room, writers-room, planning-room core pipeline flow and deterministic artifacts.
- Out of scope: major Godot UI work, broad schema redesign, and multi-writer concurrency expansion.

### Metric Baseline Checks

- Stage layout checks: `ingest-room=layout_ok`, `writers-room=layout_ok`, `planning-room=layout_ok`
- Foundation dependency check: `foundation_files=ok`
- Validation readiness check: `validation_baseline=ok`
- Stage inventory check: `12` stage directories discovered

### Risk Register Initialized

- Schema drift, queue growth, non-deterministic transforms, and planning output shape divergence documented with mitigation owners.
