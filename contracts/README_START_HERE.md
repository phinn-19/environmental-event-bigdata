# Big Data — Project Contract v1 bundle

This package contains **PROPOSED** shared contracts and small non-secret examples for the three-person project. All three people must review before freezing v1.

## Suggested repo destinations

- `contracts/PROJECT_CONTRACT_V1.md`: responsibilities, architecture, collaboration.
- `contracts/DATA_CONTRACT_V1.md`: typed Silver/Curated/Event schema and rules.
- `contracts/DATA_ACQUISITION_V1.md`: exact fields/variables to acquire; raw file layout and manifest.
- `configs/project.example.yaml`: non-secret example configuration. Do not treat candidate integration dates as proven.
- `fixtures/events/events_mock.json`: **SYNTHETIC TEST ONLY** fixtures for the dashboard, NOT actual environmental events.

## First group meeting

1. Review **MUST** fields and whether the initial MVP is PM2.5-only.
2. Verify actual overlapping history with OpenAQ / FIRMS standard or NRT product / ERA5.
3. Approve shared schema, paths, UTC and precipitation time-alignment.
4. Assign module owners and reviewers; pin PySpark + Java versions only after environment compatibility test.
5. Merge contracts and fixtures to `develop`, create the 3 feature branches, start with independent test fixtures and small PRs.

Secrets must never be committed. Files under `data/` are local artifacts, and provider data usage/attribution must be reviewed before publishing real samples.
