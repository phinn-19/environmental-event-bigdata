# Sprint 11B — verified cloud-to-ephemeral Raw restore

This small extension lets a **fresh** GitHub Actions runner read the private
Hugging Face Storage Bucket `CourseworkBigData/env-event-lake` at `lake/v1/`,
reconstruct the exact `data/raw/...` structure, and verify **actual Raw SHA-256
bytes** against manifest-v1. No provider API requests, bucket uploads/deletes,
or Process/Spark work occur. The shared `pilot/` snapshot is never accessed.

## Trust boundaries and run modes

* Default command without `--approve-download`: OFFLINE, no token access,
  network I/O or Raw mutations.
* With `--approve-download`, a **read-only scoped** `HF_TOKEN` is required.
  It rejects a public bucket, invalid paths, incomplete/orphan Raw pairs,
  duplicate references, bad size/digest, non-`live` acquisitions, mismatched
  cloud bytes, and existing local files whose contents differ.
* Download all manifests then bounded Raw into a secure temporary directory.
  Check every Raw SHA-256 before making any local files visible. Create
  new local Raw **before** corresponding manifest, using atomic hard links
  without overwrite. Retry reuses matching files. An interrupted publication
  can leave unpaired local Raw; a retry may finish it, while the existing
  audit/gate will NOT mark an unpaired Raw complete.
* Scope: max 1000 objects / 250 manifests / 2 MiB manifest metadata, max
  **8 MiB payload by default** (hard cap 64 MiB). Stop for review when exceeded;
  future work must select required date/source windows as the lake grows.
* Symlinked paths under `data/raw` are rejected. Only `data/raw/...` files
  may be materialized. Existing local Raw is never rewritten or deleted.

## Local verification (from repository root, PowerShell)

```powershell
.\.venv\Scripts\python.exe -m unittest discover -s tests -p 'test_ingestion_cloud_restore.py' -q
.\.venv\Scripts\python.exe -m unittest discover -s tests -p 'test_ingestion*.py' -q

# Safe offline sanity check:
.\.venv\Scripts\python.exe -m src.ingestion.cloud_restore
```

No token needs to be pasted into the terminal for those three commands.

## Actual restore from GitHub (after merging to develop)

The existing `.github/workflows/ingestion-cloud-pilot.yml` job now has an
**optional** cloud restore step. It uses the already configured Repository
Secret `HF_READ_TOKEN` as `HF_TOKEN`, calls:

```bash
python -m src.ingestion.cloud_restore --approve-download --max-bytes 8388608
```

Then runs the **offline** Daily Cloud Workflow for ERA5/HANOI 2024-11-05
(with `--as-of 2024-11-06 --lag-days 1 --lookback-days 1`) and asserts:

```text
CLOUD RESTORE: PASS (raw=1, manifests=1, created=2, reused=0, bytes_verified=15750)
LOCAL COMPLETE: 1; PENDING: 0; ACQUIRED THIS RUN: 0
```

The first line applies to an empty runner and the currently verified single
ERA5 pair. If the data volume grows, review the scope instead of increasing
byte limits blindly. The step performs **local writes to the ephemeral runner**
only; `READ ONLY` refers to Hugging Face cloud operations.

## Intentional limitations

* **No source API** and **no cloud uploads** in Actions yet.
* An offline daily preview using today's date may show `PENDING`; this only
  reflects the current local window and does not mean that cloud data is missing.
* Restoring *all* current `lake/v1/data/raw` is a small pilot, **not** the
  final incremental cloud-state hydration strategy. Sprint 11C must select
  source/date windows, handle historical gaps and `BLOCKED_REVIEW`, coordinate
  one cloud writer and only then consider guarded live ingestion.
* `main` remains default branch; scheduler events still require promotion to
  default branch. This change does NOT enable unattended daily source calls.
