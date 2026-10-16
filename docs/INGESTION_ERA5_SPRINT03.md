# Sprint 03 — ERA5 raw ingestion (for `feat/ingestion`)

**Status:** offline test of GRIB1 framing and mocked CDS calls; live CDS retrieval still requires verification on the user's Windows machine. This is raw acquisition only, **not** weather Silver/Parquet processing.

## New and updated files

- `src/ingestion/era5.py` — daily request construction, blocking CDS retrieve, GRIB integrity and ecCodes field/time completeness checks, raw + manifest.
- `src/ingestion/cli.py` — `--source era5` and repeated optional `--variable` for small pilot downloads. Existing FIRMS/OpenAQ commands preserved.
- `src/ingestion/dotenv.py` — optional safe load of `OPENAQ_API_KEY` and `FIRMS_MAP_KEY` from local `.env`; existing process env takes priority. CDS continues using `~/.cdsapirc`.
- `tests/test_ingestion_era5.py` — offline unit tests, no CDS credentials required.

## Install and test (Windows PowerShell)

```powershell
# Must be on feat/ingestion. Backup CLI before extracting ZIP.
New-Item -ItemType Directory -Force .\data\backups\sprint03 | Out-Null
Copy-Item .\src\ingestion\cli.py .\data\backups\sprint03\cli_before.py
Expand-Archive -LiteralPath "$env:USERPROFILE\Downloads\ingestion_era5_sprint03_patch.zip" -DestinationPath . -Force
.\.venv\Scripts\python.exe -m unittest discover -s tests -v
.\.venv\Scripts\python.exe main.py doctor
```

**Dependencies:** `cdsapi` and `eccodes` must be installed for ERA5 replay/live. They were already installed in the user's `.venv` during earlier tests; others must install them separately pending team approval of `pyproject.toml` extras. Foundation/OpenAQ/FIRMS tests don't require either one.

## Step A — replay a real one-day, 24-hour, one-variable file (NO NETWORK)

`data/sample_era5_vietnam_day.grib` is real ERA5 GRIB edition 1 (24 GRIB messages, 2 m temperature, 15 Jan 2025). This file is **not included in the patch ZIP**, because it is a local provider sample already on the user's PC.

```powershell
.\.venv\Scripts\python.exe -m src.ingestion.cli --source era5 --region VN_BBOX --start 2025-01-15 --end 2025-01-16 --variable 2m_temperature --replay-file data/sample_era5_vietnam_day.grib
```

Expected:

```text
ERA5 REPLAY (NO NETWORK): days=1, grib_messages=24, failures=0
Raw GRIB + manifest: data/raw/era5/ (Git-ignored). No Silver/Parquet output.
```

We pass `--variable 2m_temperature` because this sample contains only **one** of the **five required** MVP variables. Replay is evidence of I/O and provenance, **not** completion of 5-variable acquisition.

## Step B — live one day, one variable, small region (CDS may queue)

```powershell
Test-Path "$env:USERPROFILE\.cdsapirc"
.\.venv\Scripts\python.exe -m src.ingestion.cli --source era5 --region HANOI --start 2025-01-15 --end 2025-01-16 --variable 2m_temperature
```

CDS submissions are **blocking**; depending on server load, the command can take several minutes or longer. Do not repeatedly submit while a job is queued. We do not yet persist a remote request ID/resume interrupted jobs; a failed job records a sanitized error manifest.

## Step C — one day, all FIVE variables (after pilot PASS)

```powershell
.\.venv\Scripts\python.exe -m src.ingestion.cli --source era5 --region HANOI --start 2025-01-15 --end 2025-01-16
```

Uses exact five variables from `configs/project.yaml`, 24 hourly timestamps and CDS `area=[north,west,south,east]`. Successful single-grid output should contain 5 × 24 = **120 GRIB messages**; the ecCodes validator rejects missing/extra/duplicate variable-hour pairs. It does not materialize Spark rows or perform precipitation-unit conversion.

## Output contract

```text
data/raw/era5/
  reanalysis-era5-single-levels/HANOI/year=2025/month=01/day=15/live-<id>.grib
  manifests/reanalysis-era5-single-levels/HANOI/year=2025/month=01/day=15/live-<id>.manifest.json
```

- `source=era5`, `source_product=reanalysis-era5-single-levels`, `format=grib`.
- `row_count` means **GRIB message count**, not spatial grid cell count or Spark rows.
- A valid file must be GRIB1/GRIB2 framed and pass ecCodes metadata completeness for all requested variables and hours. Checksums are SHA-256 of untouched raw bytes.
- Replay and live have different request IDs/paths. Different bytes cannot overwrite an existing raw file unless `--overwrite` is used.
- Do not publish provider sample data until use conditions have been checked. All `data/raw/**` are gitignored.
- This version **does not** provide full CDS job-resume/status polling or historical continuity verification. That work may be added after a working daily pilot.

## Data-source references

- https://cds.climate.copernicus.eu/datasets/reanalysis-era5-single-levels?tab=download
- https://confluence.ecmwf.int/pages/viewpage.action?pageId=656872232
