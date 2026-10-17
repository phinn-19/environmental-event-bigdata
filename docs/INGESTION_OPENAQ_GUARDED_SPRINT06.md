# Sprint 06 — Guarded OpenAQ One-Day Ingestion v1

Scope: **OpenAQ hourly only**, one sensor, one UTC day per run. The existing
OpenAQ adapter, CLI, Raw layout, manifest-v1 schema and team Contracts remain
unchanged. FIRMS and ERA5 live backfill are **not** implemented here.

## Why

The legacy adapter can write a successful first page before a subsequent page
fails, and an unrestricted rerun can conflict with existing Raw or replay
provenance. This module runs an offline manifest-aware gate first, then acquires
a local lock and repeats the audit. Only a `not_found` day can proceed.

On approved runs, OpenAQ writes to a temporary staging root. The module checks
that the full staged day passes the existing audit (identity, SHA-256, pagination)
and rechecks the real destination. It then copies Raw and matching manifests
via exclusive-create (`xb`), never replacing preexisting bytes. It validates
the final published result again. A write failure attempts a rollback of files
created by the invocation; a process crash can leave partial artifacts, which
must be handled as REVIEW, not silently resumed.

## Security and operational limits

- **Default is dry-run**; API calls require explicit `--approve-live` and a
  valid `OPENAQ_API_KEY` (loaded via the project's environment loader).
- Maximum one UTC day and one sensor per run; 1–10 HTTP requests and at most
  16 MiB received; defaults: 3 requests and 2 MiB. HTTP read is bounded by
  remaining byte budget; adapter retries are disabled for this first release.
- The `_locks` directory prevents concurrent instances **of this new runner**
  from writing the same sensor/day. It is **not** a distributed lock and does
  not block the old `src.ingestion.cli` or writers on other machines.
  Operate with only one Ingestion writer for a shared Raw root.
- The temporary `_staging` directory and lock directories are created only
  after explicit live approval on a `not_found` candidate. Temporary staged
  downloads are discarded after failure. If the process crashes and leaves a
  `.lock`, inspect the state and running processes before manually removing it.
- `complete` means matching manifest/Raw identity and checksum, **not**
  scientific quality, API freshness, verified sensor coverage, or Spark E2E.
- Do not run `--approve-live` on the 2024-11-01 pilot; it is `replay` and
  deliberately held for review. Existing live-complete days are skipped.
- `REVIEW`, bad manifests, byte/request limit, provider failures or page gaps
  must **never** automatically overwrite existing Raw.

## Commands (offline first)

```powershell
.\.venv\Scripts\python.exe -m unittest discover -s tests -q

# Read-only: NO API, no Raw writes
.\.venv\Scripts\python.exe -m src.ingestion.backfill_live --region HANOI --sensor-id 21632 --start 2024-12-15 --end 2024-12-16
```

**Do not add `--approve-live` until a separate, explicit decision to test live
OpenAQ access.** Once approved, the only live mode is this same command plus
`--approve-live`, optionally with tighter `--max-http-requests 1 --max-bytes
524288`; provider availability for any historical date is not guaranteed.

Run the existing offline `backfill_audit` after any live run and inspect
`data/raw/openaq/...` before handing new data to Processing.

## Automated verification

`tests/test_ingestion_backfill_live.py` uses a fake HTTP transport with the
**real OpenAQ adapter** and isolated temporary filesystems. It checks success,
skip, replay HOLD, pagination, HTTP failure, invalid JSON, request/byte budget,
lock contention, intervening writer, publish rollback and read limit. Tests
never access provider APIs or change local project Raw.
