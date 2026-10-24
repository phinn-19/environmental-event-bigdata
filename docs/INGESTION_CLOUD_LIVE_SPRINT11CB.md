# Sprint 11C-B — Reviewed one-day OpenAQ Cloud Acquisition Pilot

This is a **manual one-time integration test**, **not daily scheduled ingestion**.

## Confirmed scope

- **Source**: OpenAQ v3, sensor `21632`, region `HANOI`.
- **UTC acquisition window**: `[2024-11-05, 2024-11-06)` using `as_of=2024-11-06`, `lag_days=1`, `lookback_days=1`.
- Cloud prefix: `CourseworkBigData/env-event-lake/lake/v1/`.
- At most **one candidate day**, **three OpenAQ HTTP requests**, **2 MiB** of source HTTP response data, and **4 MiB** of total published cloud objects. Data may be unavailable/empty; no fabricated success guarantee.
- Before any provider request, GitHub restores cloud Raw+Manifest (up to 8 MiB, immutable if conflicting). Daily Planner and the workflow's guarded executor audit state again.
- On missing/partial/conflicting data, fail closed. Provider errors, flags, source availability and quotas can block the test. No automatic retries of failed runs.
- Same GitHub concurrency group as the existing cloud tick, `environmental-event-cloud-tick-single-writer`. This is **not** a distributed cross-repo/laptop lock; only one publisher/operator should be active across all environments.

## Before manual live run

1. Merge the feature into `develop`, confirm **preview-only** `push` CI succeeds.
2. The `workflow_dispatch` entry must exist on **default branch** `main` to run manually using GitHub's UI; propose a team-reviewed PR from `develop` into `main` before attempting this. Do **not** change the default branch just for this pilot.
3. Add repository Actions Secrets `HF_READ_TOKEN` (already configured), `OPENAQ_API_KEY` (OpenAQ API key), `HF_WRITE_TOKEN` (new separate HF token scoped to Organization; write-capable only for controlled pilot). Never paste secrets in logs, YAML or PRs. Fine-grained org-scoped write access may be broader than one bucket on the Free plan; test and revoke if unnecessary.
4. In Actions, select *Sprint 11C-B - Manual OpenAQ cloud acquisition pilot* → **Run workflow**, choose `mode=live_acquire` and set **both** approval booleans to true. The selected run ref must point to reviewed code. If default branch policy blocks this, **do not work around by adding live calls to a push trigger**.
5. Inspect log: Source plan must be `PASS`, then a successful live attempt must end in `DAILY CLOUD WORKFLOW: PUBLISHED` and `CLOUD VERIFIED`. If it ends STOP/FAILED, investigate; do not automatically retry.

## Safeguards / limitations

- Default `mode=preview`: any `push` event on `develop` can ONLY restore and preview. **No schedule** or unattended provider API.
- `live_acquire` requires **two independent manual confirmations**. Missing secrets cause failure without source API calls.
- `HF_READ_TOKEN` is only used for restoration. `HF_WRITE_TOKEN` and `OPENAQ_API_KEY` exist only in the gated live step.
- A second approved run with the exact same date must reuse already COMPLETE cloud objects instead of duplicating records; review the output. **Never intentionally run two separate writers at once.**
- No processing/dashboard jobs. Historical OpenAQ sensor behavior not guaranteed in 2026, and the exact source date is fixed for this first demonstration.
- HF bucket only stores data; Github-hosted compute has resource and execution limits. A full daily schedule and long-running ERA5 downloads require a separate operational design/review.
