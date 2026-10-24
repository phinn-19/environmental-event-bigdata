# Sprint 11C-A — Controlled GitHub Cloud Tick (NO live provider calls)

Purpose: provide the **next safe increment**, not falsely claim that daily cloud
acquisition is production-ready. The live acquisition policy for ERA5, OpenAQ
and FIRMS, cross-machine writer isolation and provider credentials are **not**
yet certified for unattended use.

New workflow: `.github/workflows/ingestion-cloud-tick.yml`.

## How it behaves

1. On a push to `develop` that changes its file or test, a GitHub-hosted
   Ubuntu runner verifies 12 policy tests, chooses historical UTC
   `as_of=2024-11-06` and restores `lake/v1/` from HF with `HF_READ_TOKEN`.
2. It performs a strictly offline, one-day ERA5 preview. For the verified
   pilot partition (2024-11-05), expected:
   `LOCAL COMPLETE: 1; PENDING: 0; ACQUIRED THIS RUN: 0`.
3. The `workflow_dispatch` interface supports `preview` (default) and
   `publish_existing` (only when explicitly confirmed). A write token
   `HF_WRITE_TOKEN` is referenced **only** in the conditional cloud-only
   publish step. The step refuses to run unless the preview reports exactly
   one local COMPLETE window, zero pending, and zero acquisition.
4. No `--approve-live` is used. There is no source API call, scheduled trigger,
   new-data acquisition, cloud deletion, or unconditional write path.
5. The repo's `main` is currently the default branch. GitHub requires
   `workflow_dispatch` workflows to exist on the default branch, so a merge
   to `develop` allows **only the push preview** to run. Manual mode requires
   a separately reviewed PR to `main` (no unilateral default-branch changes).

## Limits and important safeguards

- Exactly ERA5/HANOI, one UTC day, at most one candidate and one MiB of cloud
  upload; `cloud_restore` readback is limited to eight MiB.
- Must have a fine-grained read token (`HF_READ_TOKEN`) stored in GitHub
  **repository secrets**. No credentials passed as CLI arguments.
- Separate cloud write token (`HF_WRITE_TOKEN`) is required **only later**, if
  the team approves and the `publish_existing` mode is enabled on the default
  branch. DO NOT create or add it solely for this read-only checkpoint.
- No live provider credentials or live source API approvals in this workflow.
- Concurrency group blocks overlapping runs **of this workflow**, but does not
  prevent another PC/workflow from publishing simultaneously; manual writer
  coordination remains mandatory.
- `schedule` is intentionally absent. Scheduled read-only tests in the older
  pilot (when moved to default branch) must not be mistaken for live ingest.
- A failed cloud restore, checksum conflict, missing secret or changed plan
  fails closed. The bucket is private; source data stays immutable.

## Windows local validation

```powershell
.\.venv\Scripts\python.exe -m unittest discover -s tests -p 'test_ingestion_cloud_tick_ci.py' -q
.\.venv\Scripts\python.exe -m unittest discover -s tests -p 'test_ingestion*.py' -q
```

After the PR into `develop`, inspect the new Actions workflow, confirm the
restore and preview complete and verify that provider calls are still zero.

## Next (11C-B, separately reviewed)

Design bounded **manual live** collection with source availability policies,
provider-specific credentials, cloud write token, explicit approvals, and a
strong enough single-writer strategy for the chosen deployment. Run a complete
new-day acquisition + SHA-256 + upload once on a hosted runner. Only after
successful idempotent live pilot should we review schedule enablement on
`main`, and only if quota/cost constraints are documented.
