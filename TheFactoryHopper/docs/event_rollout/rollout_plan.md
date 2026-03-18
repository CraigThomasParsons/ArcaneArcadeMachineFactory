# Event Rollout Plan

## Scope

This plan governs cutover from mixed legacy signaling and canonical event signaling
into canonical-only enforcement for Arcane stage workers.

## Rollout Flags

- config file: `TheFactoryHopper/docs/event_rollout/config.json`
- emit_mode values:
  - `legacy_only`: write legacy chatroom signals only
  - `shadow`: write legacy and canonical signals in parallel
  - `contract_only`: write canonical signals only
- projection_mode values:
  - `legacy`: disable canonical projection rebuild and use legacy status snapshot
  - `shadow`: allow canonical projections for verify-phase checks
  - `enforce`: canonical projections are authoritative

## Phase Gates

1. Shadow Emit
- command: `python stage_workers.py event-rollout-set --phase shadow`
- required checks:
  - `python stage_workers.py event-rollout-status`
  - `python stage_workers.py event-rollout-verify-parity --write-report`
- gate condition: parity report generated and reviewed

2. Verify
- command: `python stage_workers.py event-rollout-set --phase verify`
- required checks:
  - `python stage_workers.py event-rollout-verify-parity --write-report --fail-on-mismatch`
  - `python stage_workers.py event-integrity-check`
  - `python stage_workers.py event-run-summary --rebuild`
- gate condition: parity is `true` and integrity violations are `0`

3. Enforce
- command: `python stage_workers.py event-rollout-set --phase enforce`
- required checks:
  - `python stage_workers.py event-rollout-status`
  - `python stage_workers.py event-integrity-check`
  - `python stage_workers.py event-run-summary --rebuild`
- gate condition: command suite returns `ok`, no contract regressions

## Rollback Steps

1. Switch runtime to legacy-only mode:
- `python stage_workers.py event-rollout-set --phase rollback_legacy`

2. Confirm mode and snapshot:
- `python stage_workers.py event-rollout-status`
- `python stage_workers.py event-projections-show --view sprint`

3. Capture rollback handoff package:
- `python stage_workers.py event-rollout-handoff`

## Operational Notes

- Parity intentionally ignores synthetic smoke events (`contract-event-smoke`).
- Legacy and canonical sources are compared at reason granularity, not only totals.
- Cutover evidence artifacts are written under `TheFactoryHopper/docs/event_rollout`.
