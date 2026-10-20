# Sprint 08 / Step 1 — Offline incremental ingestion preview

**Owner:** Ingestion, Person 1. This is a small read-only control-plane step,
not the automated daily pipeline yet. No adapter, Raw, manifest, contract,
Processing or Dashboard behavior is changed. It needs only Python and the
existing project dependencies, not Airflow.

The operator supplies an `--as-of` **UTC date**, a source-specific conservative
`--lag-days`, and `--lookback-days` to inspect. If `as_of=2026-10-20`,
`lag_days=2`, `lookback_days=3`, the selected intervals are the UTC days
16/17/18 October with end-exclusive final bound `2026-10-19`. **Lag is a
planning choice, NOT a guarantee of upstream publication.**

Run from repo root on Windows PowerShell:

```powershell
.\.venv\Scripts\python.exe -m src.ingestion.daily_plan --source openaq --region HANOI --sensor-id 21632 --as-of 2024-11-05 --lag-days 1 --lookback-days 7 --max-candidates 7
.\.venv\Scripts\python.exe -m src.ingestion.daily_plan --source era5 --region HANOI --as-of 2024-11-05 --lag-days 1 --lookback-days 7 --max-candidates 7
.\.venv\Scripts\python.exe -m src.ingestion.daily_plan --source firms --region VN_BBOX --product VIIRS_NOAA20_SP --as-of 2024-11-05 --lag-days 1 --lookback-days 7 --max-candidates 7
```

The old FIRMS pilot used three-day windows; the stable daily plan uses **one-day
windows**. Those requests overlap already-published Raw. The existing auditor
should return `REVIEW` (`overlapping_or_alternate_acquisition_found`) for the
one-day requests, not treat them as safe missing data. **Never run overlapping
FIRMS daily downloads on top of old multi-day windows.** This requires a
future contract-compatible overlap-aware coverage index before auto-execution.

- `SKIP`: exact matching request already complete.
- `HOLD`: manifest/data ambiguous, including overlapping FIRMS windows and
  OpenAQ replay; block entire batch.
- `CANDIDATE`: no exact matching request; **NOT permission to download**.
- Entire batch blocks if any HOLD or candidate count exceeds budget.
- `--format json` yields a machine-readable plan for future DAG work.
- No provider calls, network, Raw writes, download credentials or secret logs.

## Before enabling unattended live execution

1. Each provider needs a separately reviewed availability/latency policy,
   bounded timeouts/quotas, persisted outcomes, replay/backfill treatment and
   no-overlap rules for FIRMS NRT/SP. Existing single-window guarded runners
   already re-audit before publication; do not bypass them.
2. Define the one-writer deployment and recovery policy for local locks, and
   handle partial multi-file publication as HOLD/manual review.
3. Person 2 and Person 3 must expose stable idempotent commands, input/output
   contracts and success markers. Validate manual E2E first.
4. Only then create a DAG/scheduler and enable explicit unattended credentials.
   Scheduling this read-only CLI alone does not ingest or process anything.

No `--approve-live` option exists in this preview CLI by design.
