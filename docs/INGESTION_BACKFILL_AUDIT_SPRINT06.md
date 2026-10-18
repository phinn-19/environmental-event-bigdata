# Sprint 06 — Manifest-aware Backfill Audit v1 (OFFLINE)

Owner: Member 1 / Ingestion. This **adds** a new read-only auditor and tests.
No changes to existing `backfill.py`, adapters, manifest schema, configs, or other members' code.

## Scope

The auditor reads the plan from `src.ingestion.backfill`, then checks local
`data/raw/**` and the existing manifest-v1 contract. It never calls a provider
API, changes a manifest, writes Raw, schedules a job, or executes any backfill.

Statuses:

- `COMPLETE`: exact planned identity is represented by a successful/empty
  manifest and a byte-identical SHA-256/size-matching file; basic source
  completeness checks also pass (OpenAQ terminal page, ERA5 message count).
  This is **manifest-level completeness**, NOT independent verification of
  scientific quality, GRIB semantic content, or coverage of all real sensors.
- `REVIEW`: existing data or metadata needs manual inspection (e.g. OpenAQ
  replay versus intended live request; SHA mismatch; incomplete pagination;
  FIRMS overlapping window). Never auto-overwrite these artifacts.
- `NOT_FOUND`: no exact matching local acquisition was found. This **does not**
  guarantee the provider has data or that a download is safe/authorized.

The auditor checks ONLY the `live` request identities for now. In particular,
OpenAQ raw filenames are shared between replay and live in the current adapter,
so replay files deliberately show `REVIEW` rather than being silently accepted
as completed live requests. Do not run existing CLI with `--overwrite` to "fix"
this without a reviewed migration plan.

## Commands (PowerShell, from repo root)

```powershell
.\.venv\Scripts\python.exe -m unittest discover -s tests -q
.\.venv\Scripts\python.exe -m src.ingestion.backfill_audit --source openaq --region HANOI --sensor-id 21632 --start 2024-10-31 --end 2024-11-03
.\.venv\Scripts\python.exe -m src.ingestion.backfill_audit --source firms --region VN_BBOX --product VIIRS_NOAA20_SP --start 2024-11-01 --end 2024-11-02
.\.venv\Scripts\python.exe -m src.ingestion.backfill_audit --source era5 --region HANOI --start 2024-11-01 --end 2024-11-02
```

All time windows are `[start,end)` in UTC. The default planning cap remains
31 items; `--max-plan-items` only bounds **plan items**, not actual requests,
network usage or disk bytes. `--format json` prints a machine-readable report
to stdout without writing files.

Expected for the pilot bundle when files remain intact:

- OpenAQ 2024-10-31: `COMPLETE` (live)
- OpenAQ 2024-11-01: `REVIEW` (replay; same legacy raw path)
- OpenAQ 2024-11-02: `NOT_FOUND`
- FIRMS 2024-11-01 and ERA5 2024-11-01: `COMPLETE`

These results are sensitive to the actual manifests present on the local machine.

## Known limitations / next step

- No execution, retry/resume, scheduling, daily ingestion or bytes budget yet.
- The OpenAQ completeness proof uses the **current** configured page_limit and
  terminal-page evidence. If older raw data was fetched with a different page
  limit, it is conservatively marked `REVIEW`. When `meta.found` is an exact
  integer, it must agree with the sum of stored rows; a mismatch is `REVIEW`
  even if the last page is short. Non-numeric `meta.found` is not interpreted
  as an exact count, so `COMPLETE` still does not prove full provider coverage.
- Exact FIRMS request identity includes product, bbox, start, chunk length,
  and acquisition mode. A partially overlapping file is marked `REVIEW`, not
  counted as covering the entire planned chunk.
- ERA5 identity includes region, bbox, dataset, variables, and mode. The
  manifest's `row_count` must equal 24 × number of configured variables.
- Cryptographic hash matching verifies local payload and manifest consistency.
  It does not independently prove original provider authenticity.
- The future executor needs further safety gates: immutable successful
  manifests, explicit conflict resolution for OpenAQ live/replay, retries,
  endpoint/data availability checks, rate limits, max disk/download bytes,
  and backfill runs partitioned into bounded chunks.

Do not send this auditor's `NOT_FOUND` rows directly to a network executor.
