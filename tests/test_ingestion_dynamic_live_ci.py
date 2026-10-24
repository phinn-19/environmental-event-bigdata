"""Safety regression tests for guarded dynamic OpenAQ workflow."""

import unittest
from pathlib import Path

import yaml


WORKFLOW = (
    Path(__file__).resolve().parents[1]
    / ".github/workflows/ingestion-dynamic-live.yml"
)


class DynamicLiveGuards(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        cls.doc = yaml.load(
            WORKFLOW.read_text(encoding="utf-8"),
            Loader=yaml.BaseLoader,
        )
        cls.job = cls.doc["jobs"]["one-day-openaq"]
        cls.steps = cls.job["steps"]

    def test_triggers_and_branch_guard(self):
        triggers = self.doc["on"]
        self.assertEqual(set(triggers), {"push", "workflow_dispatch"})
        self.assertEqual(self.doc["permissions"], {"contents": "read"})
        self.assertEqual(
            self.doc["concurrency"]["group"],
            "environmental-event-cloud-tick-single-writer",
        )
        self.assertIn("refs/heads/develop", self.job["if"])
        self.assertIn("github.event_name == 'workflow_dispatch'", self.job["if"])

    def test_live_requires_both_approvals(self):
        inputs = self.doc["on"]["workflow_dispatch"]["inputs"]

        self.assertEqual(inputs["mode"]["default"], "preview")
        self.assertEqual(inputs["approve_source_api"]["default"], "false")
        self.assertEqual(inputs["approve_cloud_upload"]["default"], "false")

        live = next(
            step for step in self.steps
            if step["name"].startswith("Explicitly approved one-day OpenAQ")
        )

        for condition in (
            "github.event_name == 'workflow_dispatch'",
            "inputs.mode == 'live_acquire'",
            "inputs.approve_source_api == true",
            "inputs.approve_cloud_upload == true",
        ):
            self.assertIn(condition, live["if"])

        self.assertIn("--approve-live --approve-upload", live["run"])

        for step in self.steps:
            if step is not live:
                self.assertNotIn("--approve-live", step.get("run", ""))
                self.assertNotIn("--approve-upload", step.get("run", ""))

    def test_utc_scope_and_budgets(self):
        text = WORKFLOW.read_text(encoding="utf-8")

        for required in (
            "dt.timezone.utc",
            "dt.timedelta(days=2)",
            "--scope-source openaq",
            '--scope-day "$SCOPE_DAY"',
            "--max-candidates 1",
            "--max-http-requests 3",
            "--max-bytes 2097152",
            "--max-cloud-bytes 4194304",
            "secrets.HF_READ_TOKEN",
            "secrets.HF_WRITE_TOKEN",
            "secrets.OPENAQ_API_KEY",
        ):
            self.assertIn(required, text)

        self.assertNotIn("schedule:", text)
        self.assertNotIn("cron:", text)


if __name__ == "__main__":
    unittest.main()
