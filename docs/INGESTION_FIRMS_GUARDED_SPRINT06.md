# Sprint 06 — Guarded FIRMS backfill v1

**Owner:** Ingestion (Nguoi 1). **Scope:** One NASA FIRMS NOAA-20 SP/NRT window (1–5 UTC days), one HTTP request. The existing `firms.py`, `manifest.py`, shared Contracts, Raw layout and Processing interface are not modified.

## Workflow

1. Run offline planner / auditor / execution gate; check product/date availability with NASA FIRMS official documentation before trying live.
2. `backfill_firms_live` without `--approve-live` never calls the provider and does not write Raw/manifest.
3. With explicit `--approve-live` and `FIRMS_MAP_KEY`, the runner checks the Gate, exclusively acquires a local request lock, re-audits NOT_FOUND, and issues at most **one HTTP attempt** with **zero retries**.
4. The response is bounded *during HTTP read* by `--max-bytes` (default 2 MiB, hard max 16 MiB); it is validated by the existing `ingest_firms()` in a private staging directory.
5. Only after a strict staged manifest/checksum audit and a second destination audit are Raw and manifest exclusively published; the destination is re-audited. Normal publication failures roll back files created by this invocation. Crashes midway can leave artifacts requiring manual REVIEW, not automatic overwrite.

**Manifest states:** `complete` ⇒ SKIP; `review` ⇒ HOLD; `not_found` ⇒ CANDIDATE (does **not** authorize live access). Empty CSV (valid header only) counts as `empty` and can be COMPLETE, not a network failure. The source product SP versus NRT remains explicit and is never mixed together.

## Commands (PowerShell, repo root)

Check an already known pilot day in OFFLINE mode (no API):

```powershell
.\.venv\Scripts\python.exe -m src.ingestion.backfill_audit --source firms --region VN_BBOX --product VIIRS_NOAA20_SP --start 2024-11-01 --end 2024-11-02
```

Dry-run of guarded FIRMS: **no approve flag**

```powershell
.\.venv\Scripts\python.exe -m src.ingestion.backfill_firms_live --region VN_BBOX --product VIIRS_NOAA20_SP --start 2024-11-01 --end 2024-11-02
```

Unit tests (fixtures + mock HTTP transports only):

```powershell
.\.venv\Scripts\python.exe -m unittest discover -s tests -q
```

Live requires a separate explicit `--approve-live` opt-in, a **currently confirmed available product/date**, quota/credentials and exclusive writer on the data root; no live command is prescribed in this patch. Do not log/publish FIRMS MAP_KEY, `.env` or downloaded provider data to Git.

## Known limitations

- No bulk executor, no automatic request scheduler and no managed daily incremental acquisition in this patch.
- The exclusive lock works for this runner only, not legacy CLI, remote machines, or crashed stale lock recovery. **Use one writer per data root.** Never force-delete an unexpected stale lock without inspecting artifacts.
- Publishing a CSV and its manifest is not a multi-file atomic transaction on disk; a process/OS crash between files is detected by a later audit and requires manual review.
- Per-response size ceiling is not an overall disk quota; no server-side content-length guarantees.
- `COMPLETE` verifies manifest identity and SHA; it is not scientific quality validation. BBOX is a rectangle that can extend outside Vietnam.
- NASA FIRMS SP historical availability and NRT coverage must be checked before running live; empty data must not be converted into invented fire events.
