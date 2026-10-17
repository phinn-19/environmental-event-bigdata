# Sprint 06 — Backfill execution gate v1 (offline)

**Scope:** Member 1 / `src/ingestion/` only. No change to the Acquisition v1 contract, Raw or manifest schema, Processing, or Events.

## Prerequisites

- `src/ingestion/backfill.py` — Planner v1.
- `src/ingestion/backfill_audit.py` — Manifest-aware Audit v1.
- Use Python 3.12, run from project root.

## Example: inspect an existing 3-day OpenAQ window

```powershell
.\.venv\Scripts\python.exe -m src.ingestion.backfill_gate --source openaq --region HANOI --sensor-id 21632 --start 2024-10-31 --end 2024-11-03 --max-plan-items 3 --max-candidates 1
.\.venv\Scripts\python.exe -m unittest discover -s tests -q
```

Expected on the user's verified local pilot: 31 Oct `SKIP`, 1 Nov `HOLD` (replay not accepted as live), 2 Nov `SKIP`. Plan state: `BLOCKED_REVIEW`; **no files modified**. A different local inventory may produce a different status.

## Actions and safety rules

| Audit result | Gate action | Meaning |
|---|---|---|
| `complete` | `SKIP` | Manifest matches the planned acquisition identity and file SHA; no download needed. |
| `review` | `HOLD` | Identity, provenance, integrity, pagination or overlapping request needs manual investigation. |
| `not_found` | `CANDIDATE` | Work may be missing; **not permission to download**. |

**Fail-closed, whole-plan rule:** if *any* HOLD appears, the entire plan is blocked. If candidates exceed `--max-candidates` (default 3, hard cap 100), the entire plan is blocked. `NOTHING_TO_DO` means no eligible candidates.

The gate never invokes API adapters. Its output is a read-only preflight report, **not** a manifest of provider availability and **not** a transaction log. A later executor must re-check the relevant manifests directly before dispatching an explicitly authorized operation; it must enforce limits *on actual API requests, bytes, and provider scheduling*, which this phase does not implement.

Do not pass `--overwrite` to existing ingestion adapters. OpenAQ's current live and replay paths can collide. Keep review findings for team discussion instead of silently overwriting.

### Boundaries

- Uses existing `plan_backfill` and `audit_item`. Supports OpenAQ hourly, FIRMS NOAA-20 SP/NRT, ERA5 request identities as defined by their adapters.
- No network, downloads, Raw writes, manifest writes, shared config edits, Spark, Events or Dashboard code.
- Checks request identity/checksum/known pagination conditions; does not certify scientific quality or guarantee that data exists at the provider.
- Existing 96 tests should remain unchanged; this patch adds 15 tests.
