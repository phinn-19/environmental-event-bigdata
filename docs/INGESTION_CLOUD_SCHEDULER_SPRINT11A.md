# Sprint 11A — Cloud-based scheduling pilot (read-only)

Scope: prove that GitHub Actions can run the existing Ingestion code on a
fresh hosted runner, inspect the PRIVATE Hugging Face bucket with a READ-ONLY
credential, and generate a bounded offline daily preview. No source requests,
Raw writes, HF uploads, cloud deletion, Spark, or Events jobs are permitted.

## Why this is NOT yet automatic daily ingestion

A GitHub Actions runner starts with a clean local `data/`. Sprint 08-10
planning checks **local** Raw/Manifest completeness, not Hugging Face state.
Consequently, `PENDING` on the runner **does not imply missing in cloud**.
Never append `--approve-live` or `--approve-upload` to this Actions YAML.
Next work: safely restore exactly the required Raw/Manifest pairs from cloud,
SHA-256 validate them with the existing auditor, then re-evaluate the plan.
Preserve daily provider lag and budget policies. One cloud writer only.

## Trigger behavior

- When merged to `develop`, any future push affecting Ingestion source/test/
  config/workflow paths runs a smoke job automatically. GitHub Actions' job
  uses only read permission for repo contents.
- `workflow_dispatch` and `schedule` are default-branch events. The repo's
  default branch is currently `main`; scheduled/manual triggers won't become
  available from this file until the workflow is also promoted to `main`.
- The schedule (once promoted to `main`) is 09:17 Asia/Bangkok daily; it is
  a safe *read-only monitoring pilot*. GitHub schedules may run late or be
  dropped under load; they are not a timing guarantee.
- This file does NOT change the default branch or change any Git setting.

## Optional cloud check via a read-only token

Create a new Hugging Face token with only required READ access to the private
`CourseworkBigData/env-event-lake` bucket. Before storing it in GitHub, confirm
it can list the bucket using the CLI. Do **not** reuse a powerful personal
write-enabled OAuth token; do **not** paste any token into PRs or chat.

GitHub repository -> Settings -> Secrets and variables -> Actions -> New
repository secret -> Name `HF_READ_TOKEN`, value your scoped token. Do not use
`HF_TOKEN` as the GitHub secret name here. Without a token, the cloud probe
prints `SKIPPED (HF_TOKEN not configured; cloud not checked)` while the rest
of the job can still PASS. The `CLOUD INVENTORY: PASS` line is required before
claiming the private bucket is accessible to GitHub Actions.

The cloud probe only lists metadata under `lake/v1/data/raw/` and downloads
small manifest JSON for **read-only consistency checking**. It verifies each
manifest references an existing Raw object of the same declared byte size,
and rejects orphan Raw, missing Raw, bad paths or malformed checksums. It
DOES NOT download large Raw files or verify their actual SHA-256 bytes. If
an upload was interrupted, this probe can fail until the publisher repairs
it; that's intentional.

Bounded pilot: <= 1000 objects, <= 250 manifests, <= 2 MiB total manifest
bytes. Limits must be revisited for a larger lake. The pilot never touches
`pilot/20241029_20241105/`, only `lake/v1/data/raw/`.

## Local acceptance (Windows PowerShell)

From project root and the new Sprint 11 feature branch:

```powershell
.\.venv\Scripts\python.exe -m unittest discover -s tests -p 'test_ingestion_cloud_scheduler_probe.py' -q
.\.venv\Scripts\python.exe -m unittest discover -s tests -p 'test_ingestion*.py' -q
```

Optional cloud read from Windows (only if HF_TOKEN has been temporarily set
in the terminal; avoid logging it):

```powershell
.\.venv\Scripts\python.exe -m src.ingestion.cloud_scheduler_probe
```

No secret required for GitHub job's unit tests or offline planner preview.

## Acceptance evidence

- Local: cloud probe unit tests + all ingestion tests PASS.
- GitHub: Actions run from `develop` after PR merge completes, tests and
  offline daily preview PASS, no secret exposure.
- If `HF_READ_TOKEN` exists: `CLOUD INVENTORY: PASS (...)` in Actions log.
- Full automation approval deferred until state-restoration, current-source
  readiness, cloud locking and at least one source-to-cloud live run are proven.
