# Synthetic algorithm scenarios (Contract Decisions v1.1)

These JSON files are **test fixtures**, not real observations or validated environmental events.
They define example input and expected output behaviour so member 2 and member 3 can develop independently.

- `aq_baseline_cases.json`: same UTC hour, prior 30 days, excluded null/flagged/out-of-window, usable and insufficient-history cases.
- `aq_anomaly_cases.json`: two qualifying consecutive hours; null interrupts a sequence.
- `fire_cluster_cases.json`: two nearby high/nominal-confidence VIIRS hotspots, plus far, low-confidence and late exclusions.

Initial Foundation tests verify the examples' internal rules. **They do not test real Spark, ST-DBSCAN, or event detection implementations.** Each owner must add unit tests against production code once implemented.
