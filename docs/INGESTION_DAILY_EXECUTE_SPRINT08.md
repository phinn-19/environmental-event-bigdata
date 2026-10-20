# Sprint 08 / Step 2 — Opt-in bounded daily ingestion executor

This is a **manual tick executor**, not a scheduler/DAG or full data availability
service. It reuses the reviewed stage/validate/exclusive-publish runners from
Sprint 06. **No code** in Processing or Events is changed. It does **not**
trigger Spark or refresh the Dashboard.

## Basic usage from the repo root (PowerShell)

Dry run (no network, credentials read, Raw changes, or lock file):

```powershell
.\.venv\Scripts\python.exe -m src.ingestion.daily_execute --source era5 --region HANOI --as-of 2024-11-05 --lag-days 1 --lookback-days 7 --max-candidates 2
```

For a carefully selected missing historical day and **only after checking the
read-only planner output**, an explicitly approved invocation can acquire at
most the candidate budget, one request at a time:

```powershell
# EXAMPLE ONLY — calls external services and can publish Raw
.\.venv\Scripts\python.exe -m src.ingestion.daily_execute --source openaq --region HANOI --sensor-id 21632 --as-of 2024-11-06 --lag-days 1 --lookback-days 1 --max-candidates 1 --approve-live
```

**No permanent approval** is saved. `--as-of` is an explicit UTC day. Each source
has its own publication lag; `--lag-days` is a conservative policy choice,
**not proof of provider availability**. The scheduler in the next stage must
supply an independently-reviewed lag/policy for each source. Never enable live
scheduled execution just by reusing an example command.

## Safety invariants

- Entire run is blocked without any provider calls when the gate finds HOLD
  (including historical OpenAQ `replay`, overlapping FIRMS windows), too many
  candidates, or a previous/stale daily-executor lock.
- Re-evaluates all plan items after obtaining a **local single-writer lock**,
  re-audits each item before dispatch, and re-audits again after publication.
  A failed item stops the batch. Previously successful items are skipped on the
  next invocation. Failures and completed items are summarized on stdout.
- All provider calls go through the **existing guarded runners**. OpenAQ can
  make at most `--max-http-requests` HTTP attempts **per candidate day**;
  FIRMS is at most one HTTP request per candidate window; ERA5 at most one
  `cdsapi.retrieve` invocation per day but the library may internally retry.
- `--max-candidates` is a whole-run budget (`1..3`). `--max-bytes` is
  **per item** (OpenAQ/FIRMS default 2 MiB, ERA5 default 8 MiB). ERA5's byte
  check is POST-download, not a network bandwidth limit. Running an entire
  batch could transfer more than one item's byte budget.
- Local locks do **not** coordinate with old `main.py` ingestion CLI, other
  computers, or shared filesystems. **One writer per data root** is required.
  A stale lock after crash must be investigated, not automatically deleted.
- A crash during multi-file publishing may yield partial files and a HOLD;
  this coordinator never automatically repairs/deletes suspicious Raw.
- FIRMS SP and NRT are distinct products; do not merge observations blindly.
  Old three-day FIRMS requests are intentionally HOLD when overlapping a
  planned one-day request. A future overlap-aware coverage index is needed.
- Do not run live jobs for all sources every day until availability, lateness,
  rate limits, costs, and cross-source E2E contracts have been approved.

## What remains

1. Formal availability policy and retry monitoring for each data provider.
2. Airflow DAG / Windows Scheduler once unattended live jobs are approved.
3. Person 2: stable Spark CLI, idempotent output, success markers.
4. Person 3: event job CLI and Dashboard publication semantics.
5. Team manual E2E acceptance before scheduling the full pipeline.
