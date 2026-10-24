# Sprint 09 / HF Cloud Publisher v1 — safe one-request opt-in

This tool is **not yet a scheduler or DAG**. It publishes one previously
verified LIVE acquisition (OpenAQ daily pages, ERA5 one day, or FIRMS exact
window of 1–5 days) from `data/raw` to a **private Hugging Face Storage Bucket**.
Processing and Dashboard files are not touched. It never uses provider APIs.

## Path contract

Local `data/raw/era5/.../file.grib` maps to
`hf://buckets/CourseworkBigData/env-event-lake/lake/v1/data/raw/era5/.../file.grib`.
Local manifest `file` remains **`data/raw/...` relative to the project root**.
This differs from the earlier, immutable `pilot/20241029_20241105/` snapshot.

## Windows PowerShell — preview, then explicit upload

Run from the repository root after getting a clean branch from `develop`:

```powershell
# Preview only: NO network, Hugging Face auth read, file writes, or API calls
.\.venv\Scripts\python.exe -m src.ingestion.cloud_publish --source era5 --region HANOI --start 2024-11-05 --end 2024-11-06

# ONLY after preview passes, opt in to upload this ONE verified day
.\.venv\Scripts\python.exe -m src.ingestion.cloud_publish --source era5 --region HANOI --start 2024-11-05 --end 2024-11-06 --approve-upload

# Rerun to prove idempotency (cloud is accessed but no different content is overwritten)
.\.venv\Scripts\python.exe -m src.ingestion.cloud_publish --source era5 --region HANOI --start 2024-11-05 --end 2024-11-06 --approve-upload
```

To publish an OpenAQ live day: add `--source openaq --region HANOI --sensor-id 21632`
and its exact UTC `--start`/`--end`. To publish FIRMS, use the **original**
manifest window (e.g. `--source firms --region VN_BBOX --product VIIRS_NOAA20_SP
--start 2024-11-02 --end 2024-11-05`), not overlapping one-day windows.
The 2024-11-01 OpenAQ replay remains `HOLD`; this v1 deliberately does not
relabel replay as live. The previous pilot snapshot already preserves it.

## Safety rules and limits

- Offline audit must be `COMPLETE` **before** cloud access. OpenAQ audit
  checks complete pagination and `meta.found` count; existing manifest schema,
  request identity, Raw SHA-256, and byte count must match.
- Dry run is the default. Only `--approve-upload` reads Hugging Face
  credentials and makes remote requests. Bucket must report `private=true`.
- Default per-command total payload cap is **16 MiB** (`--max-bytes` may
  explicitly increase it up to 128 MiB). Internal bucket cap is **20 GiB**,
  not a guarantee of zero cost; check Hugging Face plan/usage independently.
- On cloud, verify every existing object by readback SHA-256; differing
  content causes HOLD. Upload Raw, read it back, then upload manifest and
  read it back. Partial failed upload can leave a verified Raw without
  manifest; rerun checks and resumes. A manifest without Raw causes HOLD.
- No deletes, no cloud overwrites of differing verified content, no local
  Raw or Git writes, no credentials in logs. Do not pass tokens via CLI.
- **No distributed locking or cloud compare-and-swap.** Use a single writer
  for the `lake/v1/` prefix. Do not schedule simultaneous publishers or use
  manual `hf sync --delete` on this prefix. Cloud bucket writes are not a
  transaction; consumer jobs must check manifest+Raw before treating an
  acquisition as ready.
- The data is first downloaded/validated in local Raw by the existing
  guarded ingestion runner. **This v1 does not stream APIs directly to cloud**
  and is not yet automatically triggered from `daily_execute`.

## Next step after real acceptance

Wrap `daily_execute` and `cloud_publish` with a lightweight run-level
coordinator or pipeline DAG. Gate downstream Spark on confirmed cloud
publication and local/provider availability decisions, avoiding infinite
blocking from historical replay / FIRMS overlapping windows. Agree with
Person 2 on stable CLI and output-success semantics before orchestration.
