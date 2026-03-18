# Event Contract Fixture Catalog and Scenario Matrix

## Purpose

Document Task 8 self-test fixtures used to validate canonical contract conformance, idempotency protection, transition legality, and blocker linkage.

## Test Command

```bash
/home/craigpar/Code/ArcaneArcadeMachineFactory/.venv/bin/python \
  /home/craigpar/Code/ArcaneArcadeMachineFactory/TheFactoryHopper/bin/stage_workers.py \
  event-contract-self-test
```

Expected pass criteria:

- `result` equals `ok`
- `failed_count` equals `0`
- `tests_run` equals `4`

## Fixture Catalog

1. `required_event_type_conformance`
- Goal: verify every required event type can build and validate a legal envelope
- Source: `arcane_event_contract_self_tests.py`
- Assertion: generated type set equals `REQUIRED_EVENT_TYPES`

2. `projection_idempotency_dedupe`
- Goal: verify duplicate deliveries do not inflate throughput counters
- Fixture: duplicate `stage.work.completed` with same `idempotency_key`
- Assertion: `completed_count == 1`, `deduped_event_count == 1`

3. `terminal_transition_legality`
- Goal: verify one attempt cannot legally emit conflicting terminal events
- Fixture: `stage.work.started` then both `stage.work.completed` and `stage.work.failed`
- Assertion: at least one multi-terminal violation is detected

4. `blocker_raise_clear_linkage`
- Goal: verify raise->user_action->clear lifecycle leaves blocker closed
- Fixture: `scrum.blocker.raised`, `scrum.user_action.required`, `scrum.blocker.cleared`
- Assertion: `open_blockers == 0`, blocker status becomes `cleared`

## Scenario Matrix

| Scenario | Input Pattern | Expected Outcome |
|---|---|---|
| Event taxonomy coverage | All required event types | All envelopes validate |
| Duplicate delivery | Same idempotency key repeated | Projection dedupe prevents double count |
| Illegal terminal transition | Completed + failed for same attempt | Violation reported |
| Blocker lifecycle linkage | Raised + clear chain | No open blocker remains |

## Intentionally Rejected Transition Cases

1. `stage.work.completed` followed by `stage.work.failed` for same aggregate and attempt
- Rejected because one attempt must have one terminal outcome

2. `stage.work.failed` followed by `stage.work.skipped` for same aggregate and attempt
- Rejected because terminal states are mutually exclusive per attempt

3. `scrum.blocker.cleared` without prior raised blocker linkage
- Rejected by runtime clear guard because only open blockers can be cleared

## Pseudo-Docblock Intent Pattern

Task 8 self-tests use explicit INTENT DOCBLOCK comment sections in each test function.

Pattern:

- explain why the test exists
- explain what failure means operationally
- explain what invariant must be preserved for self-repair attempts

This is implemented in:

- `TheFactoryHopper/bin/arcane_event_contract_self_tests.py`
