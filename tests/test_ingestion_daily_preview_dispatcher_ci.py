"""Safety checks for the preview-only scheduler."""

import unittest
from pathlib import Path

import yaml


PATH = (
    Path(__file__).resolve().parents[1]
    / ".github/workflows/ingestion-daily-preview-dispatcher.yml"
)


class SchedulerSafetyTests(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        cls.text = PATH.read_text(encoding="utf-8")
        cls.doc = yaml.load(cls.text, Loader=yaml.BaseLoader)
        cls.job = cls.doc["jobs"]["dispatch-preview"]

    def test_schedule_requires_explicit_enable(self):
        self.assertEqual(
            set(self.doc["on"]),
            {"schedule", "workflow_dispatch"},
        )
        self.assertEqual(
            self.doc["on"]["schedule"][0]["cron"],
            "37 2 * * *",
        )
        self.assertIn(
            "vars.INGESTION_PREVIEW_SCHEDULE_ENABLED == 'true'",
            self.job["if"],
        )
        self.assertIn(
            "github.event_name == 'schedule'",
            self.job["if"],
        )

    def test_dispatch_targets_develop_preview_only(self):
        steps = self.job["steps"]
        self.assertEqual(len(steps), 1)

        script = steps[0]["with"]["script"]

        for expected in (
            "github.rest.actions.createWorkflowDispatch",
            "workflow_id: 'ingestion-dynamic-live.yml'",
            "ref: 'develop'",
            "mode: 'preview'",
            "approve_source_api: 'false'",
            "approve_cloud_upload: 'false'",
        ):
            self.assertIn(expected, script)

        self.assertNotIn("live_acquire", script)

    def test_isolated_permissions_and_no_secrets(self):
        self.assertEqual(
            self.doc["permissions"],
            {"actions": "write"},
        )
        self.assertIn(
            "github.ref == 'refs/heads/main'",
            self.job["if"],
        )
        self.assertIn(
            "github.repository == 'phinn-19/environmental-event-bigdata'",
            self.job["if"],
        )
        self.assertNotIn("secrets.", self.text)
        self.assertNotIn("OPENAQ_API_KEY", self.text)
        self.assertNotIn("HF_WRITE_TOKEN", self.text)
        self.assertEqual(
            self.doc["concurrency"]["cancel-in-progress"],
            "false",
        )


if __name__ == "__main__":
    unittest.main()
