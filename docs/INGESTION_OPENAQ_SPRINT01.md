# Ingestion Sprint 01 — OpenAQ hourly RAW

**Scope:** Only `openaq` has a real implementation. `firms` and `era5` still return exit code 3. No Spark, Parquet, anomaly detection or weather join is implemented here.

## Source of truth

- Data Acquisition v1: `/v3/sensors/{sensor_id}/hours` and **24-hour UTC chunks**. `period.datetimeFrom.utc`, `period.datetimeTo.utc`, `value`, `flagInfo` and `coverage` remain unmodified in the raw JSON.
- `contracts/schemas/manifest.schema.json`: canonical count field is `row_count`. The Markdown specification contained `record_count`; this patch resolves the inconsistency in favor of `row_count`. **Ask teammates to review the shared contract files in the PR.**
- `acquisition_mode=live` is a new API request; `acquisition_mode=replay` only reuses a previously downloaded sample. **Do not claim a replay to be newly acquired data.**

## Windows commands — from repository root on `feat/ingestion`

```powershell
# 1. All offline unit tests (also the Foundation tests)
.\.venv\Scripts\python.exe -m unittest discover -s tests -v

# 2. Replay one existing local sample, NO HTTP or API key needed.
.\.venv\Scripts\python.exe -m src.ingestion.cli --source openaq --region HANOI --start 2024-11-01 --end 2024-11-02 --sensor-id 21632 --replay-file data/sample_openaq_hanoi_hours.json

# 3. Live request ONLY when OPENAQ_API_KEY is present in the environment.
Test-Path Env:OPENAQ_API_KEY
.\.venv\Scripts\python.exe -m src.ingestion.cli --source openaq --region HANOI --start 2024-11-02 --end 2024-11-03 --sensor-id 21632
```

The live example uses a **different date** so it does not overwrite the replay artifact. If rerunning an already downloaded day and the HTTP bytes have changed, the tool returns a `raw_file_conflict` instead of silently overwriting; `--overwrite` is available only after checking that this is intentional.

**No API key should appear in the YAML, PowerShell command history, screenshot, committed files, logs or manifest.** Supply `OPENAQ_API_KEY` through the user's environment. Do not commit provider raw samples or `data/`.

## Output

```text
data/raw/openaq/
├── hours/sensor=21632/year=2024/month=11/day=01/page=0001.json
└── manifests/sensor=21632/year=2024/month=11/day=01/page=0001.manifest.json
```

Every page has a manifest: `status`, `row_count`, `sha256`, `bytes`, time window, source product, sanitized `error_type`, `request_id`, and `acquisition_mode`. `status=empty` means an actual API result with no records; `status=failed` means no trustworthy raw payload for that request. All paths are repo-relative. Errors and non-zero exit codes must not be mistaken for complete ingestion.

## Limits, intentionally not yet finished

1. Historical pilot is restricted to **31 days per CLI invocation** to avoid accidentally large requests; one UTC day is requested at a time.
2. The first sprint starts from configured pilot sensor `21632` or one/more explicit `--sensor-id` values; `--region` records the configured logical region but **does not spatially filter that sensor**. *Location discovery, location metadata, station coverage, licensing and filtering* are next sprint items.
3. NASA FIRMS NRT/SP, ERA5 5-variable CDS download and shared multi-source integration window are not completed in Sprint 01.
4. Network reliability uses timeout, at most 2 retries with bounded backoff for `408/429/5xx`, then a failed manifest. Check completeness rather than assuming availability.
5. A replay is deliberately restricted to one day and sensor; a sample JSON page is not a substitute for complete historical coverage.
6. The patch updates TWO shared contract files (`manifest.schema.json`, `DATA_ACQUISITION_V1.md`) and needs group review before merging to `develop`.
