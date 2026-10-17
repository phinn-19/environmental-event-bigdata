# Sprint 06 — Backfill Planner v1 (offline only)

Scope: Member 1 / Ingestion. Adds a **new** CLI and pure planner. Does not edit
`src/ingestion/cli.py`, adapters, project configuration, shared contracts, raw
files, or any member 2/3 modules. Existing CLI behavior remains unchanged.

## Demonstrate (Windows PowerShell at repo root)

```powershell
.\.venv\Scripts\python.exe -m unittest discover -s tests -q
.\.venv\Scripts\python.exe -m src.ingestion.backfill --source openaq --region HANOI --sensor-id 21632 --start 2024-11-01 --end 2024-11-04
.\.venv\Scripts\python.exe -m src.ingestion.backfill --source firms --region VN_BBOX --product VIIRS_NOAA20_SP --start 2024-11-01 --end 2024-11-08
.\.venv\Scripts\python.exe -m src.ingestion.backfill --source era5 --region HANOI --start 2024-11-01 --end 2024-11-04
```

The output is an *offline plan*, not a download. **No request is sent and no
files are written.** The planner checks the shared config only to reject
unknown regions and provide OpenAQ's pilot sensor default when omitted.

- OpenAQ: one plan item per sensor and UTC day, actual API page count unknown.
- FIRMS: at most five UTC days per planned request.
- ERA5: one UTC day and the configured variables per item.
- All intervals are `[start, end)`, UTC, end-exclusive.
- The `--max-plan-items` bound limits the **preview size**, NOT actual HTTP
  requests or disk bytes. Default 31, hard cap 10,000.

## Important limitations / next stages

v1 **does not read manifests**, and cannot claim a window is missing or
complete. It does **not** skip existing raw, retry, resume, schedule downloads,
or cap real network activity. Those require a distinct, tested state scanner and
executor. Do not put this planner's output directly into unattended execution.

Before any execution, verify complete OpenAQ pagination (including the terminal
page or equivalent), FIRMS region/product/request identity, and ERA5 area,
variable set, mode, and SHA-256. An `empty` FIRMS result may be valid. Avoid
mixing `live` and `replay` provenance. Failed/retried tasks must not overwrite
prior successful manifest files. For production use, use controlled chunk size,
streaming/size guards and agreement on target dates with Processing.
