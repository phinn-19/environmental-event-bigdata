# Sprint 10 — Guarded Daily Ingestion -> Hugging Face Cloud Workflow v1

One manual CLI composes the **existing Sprint 08 Daily Executor** and **Sprint
09 Cloud Publisher**. It does **not** modify either component, Spark, Events,
or Data Contracts. It is **not** a scheduler or unattended production DAG.

## Data path and two modes

`daily_plan` (read-only gate) -> `daily_execute` (only if a candidate has
explicit source+cloud approvals) -> full local `audit_item` ->
`verified_entries` -> `publish` (private HF bucket, SHA-256 readback,
Raw-before-Manifest). Output: `lake/v1/data/raw/...`.

- **Preview (default):** re-audits the plan and validates existing COMPLETE
  items locally. No provider or cloud requests, credentials or Raw writes.
  Pending candidates cannot have their exact upload size known in advance.
- **Cloud-only recovery (`--approve-upload`):** if ALL requested windows
  are already locally COMPLETE, publish/reverify them without calling any
  source provider. This is essential for retrying a failed cloud upload and
  publishing historical already-ingested Raw without refetching data.
- **Full manual tick (`--approve-live --approve-upload`):** for missing
  eligible windows, run the original bounded guarded executor and re-audit
  every planned window before publishing. Both approvals are mandatory if
  any candidate would require a provider call. No permanent approval saved.

## Manual acceptance on Windows PowerShell

From repository root, after installing `huggingface_hub` and authenticating
with `hf auth login` (already done for Person 1):

```powershell
# Offline, existing local ERA5 2024-11-05, already previously uploaded to HF.
.\.venv\Scripts\python.exe -m src.ingestion.daily_cloud_workflow `
  --source era5 --region HANOI `
  --as-of 2024-11-06 --lag-days 1 --lookback-days 1

# Cloud-only acceptance: confirms existing bytes via cloud readback;
# does NOT call cdsapi or download ERA5 again.
.\.venv\Scripts\python.exe -m src.ingestion.daily_cloud_workflow `
  --source era5 --region HANOI `
  --as-of 2024-11-06 --lag-days 1 --lookback-days 1 `
  --approve-upload

# Re-run the same command. Expect new=0, reused_verified=2.
```

Example for a **new missing day** (DO NOT run live without reviewing
provider readiness, local gate, rate limits, budgets and policy first):

```powershell
# Offline preflight for a chosen UTC date; no source/cloud API calls.
.\.venv\Scripts\python.exe -m src.ingestion.daily_cloud_workflow `
  --source era5 --region HANOI `
  --as-of YYYY-MM-DD --lag-days 2 --lookback-days 1 `
  --max-candidates 1 --max-bytes 8388608

# Only after an explicit go/no-go review, execute that exact request by
# adding BOTH --approve-live and --approve-upload to the reviewed command.
```

OpenAQ needs explicit `--sensor-id 21632`; FIRMS needs `--product
VIIRS_NOAA20_SP` (or NRT) and the configured source-specific region. This
workflow intentionally leaves `REVIEW` histories blocked (including OpenAQ
replay and overlapping FIRMS 3-day raw).

## Safety and failure behavior

- Default max one source candidate per invocation; at most 3 if explicitly
  raised. Existing executor's per-request HTTP and byte limits still apply.
- Default total 16 MiB cloud payload budget (up to 128 MiB via
  `--max-cloud-bytes`) and max 64 Raw/Manifest objects per invocation. HF
  publisher additionally checks its own bucket size safety cap, which does
  not guarantee zero provider charges.
- Existing COMPLETE data is verified locally before publication. Every
  planned request must be COMPLETE **after** execution; otherwise stop
  before accessing cloud.
- If acquisition fails or the plan changes, the workflow does not publish a
  partial batch; retry later with a reviewed window. Already-complete data
  can be published separately with an all-COMPLETE window.
- Cloud publisher verifies existing object content (not just names) and
  uploads new Raw before Manifest, with SHA-256 readback. Network interruption
  can leave Raw without manifest; re-running is idempotent. Manifest-without-
  Raw or conflicting content triggers STOP, not silent overwrite.
- No cloud deletes, no local Raw rewrites. `--approve-upload` reads Hugging
  Face credentials; do not paste tokens into logs or CLI arguments.
- One writer only: the executor's file lock is local and released before HF
  upload; there is **no distributed/cloud lock** across computers. Coordinate
  manually and do not run two publishers simultaneously.
- No automatic source availability detection; `--lag-days` is a **reviewed
  conservative policy input**, not a guarantee that ERA5/OpenAQ/FIRMS has
  published the requested period. No scheduler should invoke live mode yet.

## Verification

```powershell
.\.venv\Scripts\python.exe -m unittest discover -s tests -p "test_ingestion_daily_cloud_workflow.py" -q
.\.venv\Scripts\python.exe -m unittest discover -s tests -p "test_ingestion*.py" -q
```

Full repository tests separately require the Person 2 PySpark runtime.

## Next stage

After this manual-tick integration is accepted: (1) merge through PR into
`develop`; (2) agree on per-source lag/retry/quota; (3) select execution
runtime/scheduler with the Ingestion owner; (4) agree with Person 2 on a
stable Spark CLI, output paths, and success markers before any E2E DAG.
