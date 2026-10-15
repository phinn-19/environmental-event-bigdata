# Group workflow — v1 (3 members)

## Before creating branches

1. Everyone reads `contracts/PROJECT_CONTRACT_V1.md`, `contracts/DATA_CONTRACT_V1.md`, `contracts/DATA_ACQUISITION_V1.md` and `configs/project.yaml`.
2. Agree on table columns, output folders, UTC, FIRMS NRT/SP distinction, ERA5 hourly accumulated precipitation, and responsibility for data quality.
3. Each teammate runs `python main.py doctor` and `python -m unittest discover -s tests -v` after installing dependencies. These tests do **not** require credentials, Spark, Java, or downloaded datasets.
4. Add GitHub issues for remaining unknowns: overlap window, FIRMS SP historical access, OpenAQ quality/coverage, PySpark+Java compatibility; don't hard-code unverified answers.

## Branching

- Stable: `main` — only reviewed, integration-tested work.
- Integrate early: `develop` — PR target.
- Member 1: `feat/ingestion` — `src/ingestion` and `tests` related to ingestion.
- Member 2: `feat/processing` — `src/processing`, Spark/Parquet and corresponding tests.
- Member 3: `feat/events-dashboard` — `src/events`, `src/dashboard`, corresponding tests.

**On each member's own clone**, create *their* branch from updated `develop`:

```powershell
git switch develop
git pull origin develop
git switch -c feat/ingestion       # Member 1 only; use their own branch name otherwise
# ... work, test, commit, push
git push -u origin feat/ingestion
```

Create PR `feat/ingestion` → `develop`. Do not work together by editing the same `main.py` or central config without review.

## Definition of done for each PR

- Keeps `python main.py doctor` and `python -m unittest discover -s tests -v` passing.
- Adds tests for new behavior, including empty inputs and nulls.
- Does not change contracts unexpectedly. Breaking change needs all three approvals, fixture/schema/docs update.
- No `.env`, `.cdsapirc`, API token, `.venv`, unreviewed real datasets.
- Works on small sample; reports input format/path and output format/path.
- One other group member reviews it; merge to `develop` while changes are still small.

## Integration readiness ≠ complete system

`doctor PASS` means scaffold/config/fixtures are consistent, **not** that Spark, ingestion, event algorithms, dashboard or end-to-end real data work. Those remain acceptance tests after implementation.
