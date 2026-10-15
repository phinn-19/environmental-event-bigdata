# TEST FIXTURES — SYNTHETIC ONLY

All records under `fixtures/silver`, `fixtures/curated`, and `fixtures/events` are fabricated **solely** to test interfaces, schema, null handling, and dashboard UI.

- JSON fixture rows use UTC ISO 8601 `Z` to be easily reviewed; **production Silver/Curated outputs are Parquet with UTC timestamp type**, not JSON.
- Synthetic baseline and synthetic hotspot IDs must **never** appear in any real results, benchmarks, report findings, or environmental claims.
- `fixtures/raw/` intentionally ships **no provider data**; source licenses/attribution must be reviewed before adding real samples to Git.
- Event JSON is a list, and `[]` is valid.
- For raw GRIB from user experiments, read from local `data/` rather than commit it.

- `fixtures/scenarios/`: additional synthetic algorithm scenarios for v1.1; see their README. They are not real performance or accuracy results.
