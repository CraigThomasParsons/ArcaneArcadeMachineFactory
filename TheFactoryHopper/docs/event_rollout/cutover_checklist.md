# Task 9 Cutover Checklist

## Sprint0 Task Status

- Task 0: complete (owner: StageWorker)
- Task 1: complete (owner: StageWorker)
- Task 2: complete (owner: StageWorker)
- Task 3: complete (owner: StageWorker)
- Task 4: complete (owner: StageWorker)
- Task 5: complete (owner: TessScrumMaster)
- Task 6: complete (owner: ProjectionRuntime)
- Task 7: complete (owner: RecoveryOperator)
- Task 8: complete (owner: ContractTestRunner)
- Task 9: complete (owner: RolloutOperator)

## Task 9 Validation Checklist

- [x] Rollout flags implemented (`emit_mode`, `projection_mode`)
- [x] Shadow phase command executed
- [x] Parity report generated (`parity_ok=true`)
- [x] Enforce phase command executed
- [x] Integrity check passed (`violation_count=0`)
- [x] Run summary generated in enforce mode
- [x] Handoff package generated (`handoff_package.json`, `handoff_package.md`)

## Open Issues

- One historical open blocker remains in projections from a prior smoke event context.
- This does not break schema integrity, but should be resolved or cleared before production cutover freeze.

## Next Task

- Next Task: Sprint0 completion handoff and Sprint1 kickoff preparation.
- Recommended first command: `python stage_workers.py event-rollout-handoff`
