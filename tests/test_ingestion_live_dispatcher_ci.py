"""Safeguards for separately promoted main-only live dispatcher."""
import unittest
from pathlib import Path
import yaml

ROOT = Path(__file__).resolve().parents[1]
WF = ROOT / '.github/workflows/ingestion-live-daily-dispatcher.yml'

class DailyLiveDispatcherGuards(unittest.TestCase):
    def test_schedule_is_opt_in_and_non_default(self):
        doc = yaml.load(WF.read_text(encoding='utf-8'), Loader=yaml.BaseLoader)
        self.assertEqual(set(doc['on']), {'schedule', 'workflow_dispatch'})
        self.assertEqual(doc['on']['schedule'][0]['cron'], '47 2 * * *')
        job = doc['jobs']['dispatch-guarded']
        self.assertIn("vars.INGESTION_LIVE_SCHEDULE_ENABLED == 'true'", job['if'])
        self.assertIn("github.ref == 'refs/heads/main'", job['if'])
        self.assertEqual(doc['permissions'], {'actions': 'write'})

    def test_manual_run_is_preview_only(self):
        doc = yaml.load(WF.read_text(encoding='utf-8'), Loader=yaml.BaseLoader)
        job = doc['jobs']['dispatch-guarded']
        step, = job['steps']
        script = step['with']['script']
        self.assertIn("const live = scheduled && enabled", script)
        self.assertIn("context.eventName === 'schedule'", script)
        self.assertIn("workflow_id: 'ingestion-multisource-live.yml'", script)
        self.assertIn("ref: 'develop'", script)
        self.assertIn("source: 'all'", script)
        self.assertIn("mode: live ? 'live_acquire' : 'preview'", script)
        self.assertIn("approve_source_api: live ? 'true' : 'false'", script)
        self.assertIn("approve_cloud_upload: live ? 'true' : 'false'", script)
        self.assertNotIn('secrets.', WF.read_text(encoding='utf-8'))

if __name__ == '__main__': unittest.main()
