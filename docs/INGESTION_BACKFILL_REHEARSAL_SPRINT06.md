# Sprint 06: Safe dispatch rehearsal (offline / mock only)

This is a **simulation**, not the live Safe Executor. There is no live connector to
OpenAQ, NASA FIRMS or CDS. No APIs, Raw writes, new manifests, or secret reads
are performed by `src.ingestion.backfill_execute` CLI.

## Why

- Reuse the frozen `PlanItem` / audit / gate interfaces without modifying them.
- Gate *all* work before simulated dispatch. A HOLD or excess CANDIDATE budget
  blocks the entire plan (fail-closed).
- Require explicit `--approve-simulation` to run mock callbacks.
- Re-audit a CANDIDATE immediately before its mock callback. If the item is now
  COMPLETE or REVIEW, abort remaining mock operations. If any earlier items
  were simulated, show the partial count; do not misreport rollback.
- SKIP never dispatches, and REVIEW never dispatches.

## Windows PowerShell commands

```powershell
.\.venv\Scripts\python.exe -m unittest discover -s tests -q

# Three days: the previously identified 2024-11-01 replay must HOLD and block.
.\.venv\Scripts\python.exe -m src.ingestion.backfill_execute --source openaq --region HANOI --sensor-id 21632 --start 2024-10-31 --end 2024-11-03 --max-plan-items 3 --max-candidates 1 --approve-simulation

# Use a single day known to be genuinely absent to test SIMULATED, but it
# remains mock-only even when --approve-simulation is present.
.\.venv\Scripts\python.exe -m src.ingestion.backfill_execute --source openaq --region HANOI --sensor-id 21632 --start 2024-11-04 --end 2024-11-05 --max-plan-items 1 --max-candidates 1 --approve-simulation
```

## Limitations before any real downloads

This module provides **no live executor**. It does not reserve/lock output paths
atomically; it cannot prevent filesystem races with a separate running process.
It does not enforce provider quotas, bytes downloaded, true request counts,
CDS job duration, or daily scheduling. Those belong to a separately reviewed
live executor and adapter integration. In particular, one OpenAQ day can need
multiple pages, so `max_candidates` is *not* an HTTP request budget.

OpenAQ's `replay` and `live` use the same current physical paths. Do not
re-ingest a HOLD item or enable `--overwrite` to make it pass. Consult the team
before changing any shared raw layout or historical provenance.
