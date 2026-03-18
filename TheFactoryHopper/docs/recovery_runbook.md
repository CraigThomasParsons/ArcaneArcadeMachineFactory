# Arcane Event Recovery Runbook

## Purpose

Provide a deterministic operator sequence for recovering Arcane event-state visibility after interruption, crash, or partial execution.

## Preconditions

- Runtime event stream exists at `TheFactoryHopper/docs/event_stream/events.jsonl`
- Python environment is active for ArcaneArcadeMachineFactory
- Stage worker CLI is available at `TheFactoryHopper/bin/stage_workers.py`

## Recovery Sequence

1. Run replay rebuild from stream start

```bash
/home/craigpar/Code/ArcaneArcadeMachineFactory/.venv/bin/python \
  /home/craigpar/Code/ArcaneArcadeMachineFactory/TheFactoryHopper/bin/stage_workers.py \
  event-replay --start-line 1 --max-events 0
```

2. Inspect sprint-level health summary

```bash
/home/craigpar/Code/ArcaneArcadeMachineFactory/.venv/bin/python \
  /home/craigpar/Code/ArcaneArcadeMachineFactory/TheFactoryHopper/bin/stage_workers.py \
  event-run-summary --limit 10
```

3. Check integrity of recent canonical envelopes

```bash
/home/craigpar/Code/ArcaneArcadeMachineFactory/.venv/bin/python \
  /home/craigpar/Code/ArcaneArcadeMachineFactory/TheFactoryHopper/bin/stage_workers.py \
  event-integrity-check --start-line 1 --max-events 0
```

4. If summary is stale, force rebuild + show in one pass

```bash
/home/craigpar/Code/ArcaneArcadeMachineFactory/.venv/bin/python \
  /home/craigpar/Code/ArcaneArcadeMachineFactory/TheFactoryHopper/bin/stage_workers.py \
  event-projections-show --view sprint --rebuild
```

## Output Artifacts

- Replay/projection state:
  - `TheFactoryHopper/docs/event_projections/state.json`
- Blocker lifecycle registry:
  - `TheFactoryHopper/docs/scrum_master_signals/open_blockers.json`
- Review summary:
  - `TheFactoryHopper/docs/scrum_master_review.json`

## Common Recovery Scenarios

### Scenario: Open blocker count is unexpectedly high

- Run `event-projections-show --view blockers --limit 25`
- Cross-check blocker entries against `open_blockers.json`
- Verify corresponding clear events exist in event stream

### Scenario: Integrity command reports schema violations

- Note `stream_line`, `event_id`, and `event_type` from output
- Inspect source event line in `events.jsonl`
- Repair producer path and re-run `event-integrity-check`

### Scenario: Incomplete attempts persist

- Run `event-run-summary --limit 25`
- Inspect `incomplete_attempts.items` for stalled task ids
- Trigger stage-level remediation or rerun affected stage

## Known Edge Cases and Mitigations

1. Legacy pre-contract events may be absent from canonical stream
- Mitigation: rely on contract-era events for projection truth

2. Duplicate deliveries may appear during retries
- Mitigation: projections dedupe by `idempotency_key`

3. Malformed JSON lines in event stream
- Mitigation: loader skips malformed records; integrity check surfaces schema failures for valid parsed envelopes

## Operational Notes

- Use `--max-events` during forensic replay windows to isolate incident slices.
- Prefer full replay (`--max-events 0`) before handoff snapshots.
- Capture command output JSON in incident notes for audit continuity.
