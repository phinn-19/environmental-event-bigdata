# Sprint 06 — Guarded ERA5 backfill v1

**Owner:** Ingestion (Person 1). Only one **complete UTC day**, all 5 variables configured in `configs/project.yaml`, one synchronous CDS job. The existing `era5.py` and the Raw/manifest contract stay unchanged.

## Safety and flow

1. Run the offline manifest audit / Gate. `COMPLETE` means SKIP; any `REVIEW` means HOLD; `NOT_FOUND` is only a candidate, **not** permission to download.
2. Without `--approve-live`, the runner makes no CDS request and publishes no Raw. With the flag, it takes a local exclusive lock, re-audits, and submits at most **one** CDS request, without runner-level retries.
3. CDS writes a GRIB into staging. The file is rejected if its size exceeds `--max-bytes` (default **8 MiB**, max **128 MiB**). **IMPORTANT: THIS IS NOT AN HTTP/NETWORK BYTE CAP.** `cdsapi` may transfer the complete file and block in a remote queue *before* this guard knows the size; check expected region/variables and API quota first. The guard cannot impose a strict wall-clock CDS timeout.
4. The existing ERA5 adapter validates GRIB framing and uses **ecCodes** to confirm exactly 24 UTC hours for every requested variable (120 messages for five variables). A staged manifest SHA/identity audit must pass, then the destination is re-audited, before exclusive publication (`xb`) of Raw and manifest. Finally re-audit the destination.
5. Publication can leave a partial artifact after process or machine crashes; subsequent audits must return `REVIEW`, not silently overwrite. This lock only coordinates runs of this new runner on the same data root, not the legacy CLI or distributed writers.

## Safe commands from the repository root

```powershell
.\.venv\Scripts\python.exe -m unittest discover -s tests -q
.\.venv\Scripts\python.exe -m src.ingestion.backfill_audit --source era5 --region HANOI --start 2024-11-01 --end 2024-11-02
.\.venv\Scripts\python.exe -m src.ingestion.backfill_era5_live --region HANOI --start 2024-11-01 --end 2024-11-02
```

The known pilot day is COMPLETE and should be SKIPPED. **Do not append `--approve-live` during installation/verification.** Only schedule an intentionally selected NEW day after review of CDS data availability and quota, no other writer active, and expected GRIB size. A CDS request can take a long time and may consume provider resources.

## Scope and limitations

- No automatic multi-day batch execution or incremental scheduling is implemented by this runner.
- Injected downloaders and validators in unit tests simulate transport and GRIB semantics: passing tests does not prove a new live CDS download or scientific quality.
- `COMPLETE` is a manifest identity, SHA-256 and expected message count check; it does not re-decode the old Raw or establish scientific validity. The existing adapter performs semantic validation for newly staged downloads.
- No changes to contracts, Silver/Curated, Processing or Events. Never commit `.cdsapirc`, credentials or downloaded data.
