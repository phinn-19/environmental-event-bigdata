# Sprint 02 — NASA FIRMS NOAA-20 RAW ingestion (proposed PR)

Status: **code tested offline**, replay verified against a 10-row REAL provider sample;
**live user-machine API not yet verified**. This sprint does not implement a fire cluster or Silver/Parquet.

## Added/changed files

- `src/ingestion/firms.py` — NASA FIRMS Area CSV ingestion and raw preservation.
- `src/ingestion/cli.py` — `--source firms`, explicit `--product`, same CLI as OpenAQ.
- `src/ingestion/manifest.py` — existing `make_manifest` accepts optional `source` and `data_format`, backward compatible with OpenAQ.
- `tests/test_ingestion_firms.py` — offline, deterministic tests.
- This document.

## Install

From project root, **on `feat/ingestion`**; create backups of the two overwritten files before extracting the ZIP:

```powershell
New-Item -ItemType Directory -Force .\data\backups\sprint02 | Out-Null
Copy-Item .\src\ingestion\cli.py .\data\backups\sprint02\cli_before.py
Copy-Item .\src\ingestion\manifest.py .\data\backups\sprint02\manifest_before.py
Expand-Archive -LiteralPath "$env:USERPROFILE\Downloads\ingestion_firms_sprint02_patch.zip" -DestinationPath . -Force
```

No new Python dependencies. Test Foundation + OpenAQ + FIRMS:

```powershell
.\.venv\Scripts\python.exe -m unittest discover -s tests -v
.\.venv\Scripts\python.exe main.py doctor
```

## Step A: replay a real 10-row CSV (NO API / NO FIRMS MAP_KEY)

The earlier NASA example `data/sample_firms.csv` is VIIRS NOAA-20 NRT and contains
observations on Oct 09, 10, 11 (UTC), 2026. Choose an inclusive input range for
those dates, but CLI end is **exclusive**. All points lie inside `VN_BBOX`.

```powershell
.\.venv\Scripts\python.exe -m src.ingestion.cli --source firms --product VIIRS_NOAA20_NRT --region VN_BBOX --start 2026-10-09 --end 2026-10-14 --replay-file data/sample_firms.csv
```

Expected with that exact input sample:

```text
FIRMS REPLAY (NO NETWORK): chunks=1, raw_rows=10, empty_chunks=0, failures=0
Raw CSV + manifest: data/raw/nasa_firms/ (Git-ignored). No Silver/Parquet output.
```

If the replay CSV has been changed or is not the one from this project, row count may differ.

## Step B: live one-day request (requires API key in current terminal)

```powershell
Test-Path Env:FIRMS_MAP_KEY
.\.venv\Scripts\python.exe -m src.ingestion.cli --source firms --product VIIRS_NOAA20_NRT --region VN_BBOX --start 2026-10-14 --end 2026-10-15
```

A success or a **header-only empty** response is both valid; it depends on the actual data.
If the NASA service reports `http_401`, `http_404`, `invalid_firms_csv_header`, etc.,
inspect the sanitized manifest; do not guess that a request succeeded. Check the provider's
[data availability endpoint](https://firms.modaps.eosdis.nasa.gov/api/data_availability/)
for the selected product/date; the code does not yet query availability automatically.

The program reads `FIRMS_MAP_KEY` from **the process environment**. Merely storing it in
`.env` does not load it. Avoid sharing any key in screenshots or files; rotate any exposed key.

## Raw + manifest contract

One CSV per chunk (at most 5 UTC days):

```text
data/raw/nasa_firms/
  VIIRS_NOAA20_NRT/VN_BBOX/year=2026/month=10/<request_id>.csv
  manifests/VIIRS_NOAA20_NRT/VN_BBOX/year=2026/month=10/<request_id>.manifest.json
```

Each manifest records: `source=nasa_firms`, `source_product`, `acquisition_mode`,
`region_id`, `[start_utc,end_utc)`, `format=csv`, `file`, `status`, `row_count`,
`bytes`, `sha256`, `request_id`, `retrieved_at_utc`, `error_type`.

- `status=success`: valid CSV with one or more hotspot rows.
- `status=empty`: valid CSV containing a header and zero hotspot rows.
- `status=failed`: no valid raw output for that chunk; sanitized diagnostic code, `file=null`.
- Validates required header, UTC acquisition dates/times, bbox bounds, NRT-vs-SP mismatch.
- Keeps **all original CSV bytes and all rows**; does **not** deduplicate, filter by confidence, or cluster.
- Refuses to replace changed raw content unless `--overwrite` was explicitly passed.
- Replay and live have distinct request identities and paths (no silent overwrite).
- Never writes MAP_KEY or HTTP URL to disk or console.

## Critical FIRMS API rule

NASA Area API expects `west,south,east,north` (NOT CDS ERA5 `north,west,south,east`).
The endpoint's `DATE` is **inclusive**, but project CLI uses `[start,end)` with exclusive end.
NASA accepts `DAY_RANGE` from 1 to 5; the code chunks longer windows accordingly (max 31 days per CLI invocation).
Select product explicitly: `VIIRS_NOAA20_NRT` and `VIIRS_NOAA20_SP` are different archives.

## Limitations / next acceptance

- Live FIRMS request must be checked by the user before claiming real API acceptance.
- For historical studies, verify coverage with NASA's availability API. It may not offer all
  dates in both SP and NRT; automatic availability discovery is a future extension.
- A valid CSV with 0 hotspots is not a claim of “no fire in Vietnam”; the bbox is approximate
  and satellite detections are not equivalent to fire events.
- The manifest schema remains unchanged; its optional fields already support FIRMS.
- Update document/code through reviewed PR before merging to `develop`.

Official NASA references:
- https://firms.modaps.eosdis.nasa.gov/api/area/
- https://firms.modaps.eosdis.nasa.gov/api/data_availability/
