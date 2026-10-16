# Ingestion Sprint 01B — OpenAQ Location Metadata

**Scope:** Add lossless station/sensor metadata for OpenAQ pilot. Does not add FIRMS/ERA5, historical coverage estimates, a spatial join, or production event detection.

## What this change adds

- `GET /v3/locations/{location_id}` in live mode, matching the official OpenAQ v3 Locations endpoint.
- Read existing `/v3/locations` *list response* in offline replay mode (the user's `data/sample_openaq_locations.json` contains multiple locations).
- Verify that selected location, chosen **PM2.5 sensor**, valid coordinates, and membership in configured region exist. Preserve full original API bytes, timezone, provider, owner, licenses, other stations/sensors and `datetimeFirst/Last` as-is.
- Metadata is **not a time-windowed measurement**. In its manifest: `start_utc=null`, `end_utc=null`, `source_product=openaq_v3_locations`. Hourly manifest windows remain unchanged. A metadata `datetimeFirst/Last` field is **not proof of continuous data coverage**.
- For offline replay `row_count` counts *all location objects in the JSON `results` array*, not the single selected location or the number of sensors.

## Windows PowerShell, repository root, branch `feat/ingestion`

Run first:

```powershell
.\.venv\Scripts\python.exe -m unittest discover -s tests -v
.\.venv\Scripts\python.exe main.py doctor
```

Offline replay (no API key; requires the existing local sample):

```powershell
.\.venv\Scripts\python.exe -m src.ingestion.cli --source openaq --resource locations --region HANOI --location-id 7441 --sensor-id 21632 --replay-file data/sample_openaq_locations.json
```

Live request (must have `OPENAQ_API_KEY` in current terminal environment):

```powershell
Test-Path Env:OPENAQ_API_KEY
.\.venv\Scripts\python.exe -m src.ingestion.cli --source openaq --resource locations --region HANOI --location-id 7441 --sensor-id 21632
```

The old hourly command remains valid without `--resource`, e.g.:

```powershell
.\.venv\Scripts\python.exe -m src.ingestion.cli --source openaq --region HANOI --start 2024-11-02 --end 2024-11-03 --sensor-id 21632
```

**Output (Git ignored):** `data/raw/openaq/locations/<request_id>.json` and `data/raw/openaq/manifests/locations/<request_id>.manifest.json`. Live and replay use separate request identifiers so a list-page replay cannot be mistaken for a location-specific API response. Re-fetching location data that has changed will be refused by default; run with `--overwrite` only deliberately.

## Important limitations / teamwork

1. This implements **lookup of a selected known location**; the whole-Vietnam `/v3/locations?iso=VN&parameters_id=2` discovery + full pagination is a future task. `locations` endpoint with an ID is enough for the chosen pilot sensor.
2. Snapshot metadata does **not** establish that hourly measurements are continuous across the date span displayed in `datetimeFirst/Last`.
3. Two shared contract files change: `contracts/schemas/manifest.schema.json` broadens start/end to allow `null` for non-temporal resources, and `contracts/DATA_ACQUISITION_V1.md` documents the distinction. **Review with team before merging into `develop`.**
4. There are no new Python dependencies. No secrets are written to config, file names, manifest, docs or logs. Do not commit `.env`, files under `data/`, or unreviewed provider payloads.
5. The code rejects locations outside the *declared region*, or selected sensors that aren't PM2.5. It does not automatically compute coverage, quality, or usable stations across the country.

Official API: https://docs.openaq.org/api/operations/location_get_v3_locations__locations_id__get
