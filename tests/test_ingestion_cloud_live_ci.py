"""Static fail-closed CI guard for opt-in OpenAQ GitHub Actions pilot."""
from __future__ import annotations

import unittest
from pathlib import Path

import yaml


ROOT = Path(__file__).resolve().parents[1]
WORKFLOW = ROOT / '.github/workflows/ingestion-cloud-live-pilot.yml'


class LivePilotWorkflowTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.text = WORKFLOW.read_text(encoding='utf-8')
        # BaseLoader keeps YAML key 'on' literal (YAML 1.1 SafeLoader treats it as True).
        cls.doc = yaml.load(cls.text, Loader=yaml.BaseLoader)
        cls.steps = cls.doc['jobs']['one-day-openaq']['steps']
        cls.live = next(s for s in cls.steps if s.get('name', '').startswith('Explicitly approved'))
        cls.preview = next(s for s in cls.steps if s.get('name', '').startswith('Preview exactly'))

    def test_triggers_exclude_schedule(self):
        self.assertEqual(set(self.doc['on']), {'push', 'workflow_dispatch'})

    def test_push_only_develop(self):
        self.assertEqual(self.doc['on']['push']['branches'], ['develop'])

    def test_preview_default(self):
        self.assertEqual(self.doc['on']['workflow_dispatch']['inputs']['mode']['default'], 'preview')

    def test_live_requires_two_manual_approvals(self):
        cond = self.live['if']
        for term in ('github.event_name', "'workflow_dispatch'", "'live_acquire'",
                     'inputs.approve_source_api == true', 'inputs.approve_cloud_upload == true'):
            self.assertIn(term, cond)

    def test_booleans_default_false(self):
        inputs = self.doc['on']['workflow_dispatch']['inputs']
        self.assertEqual(inputs['approve_source_api']['default'], 'false')
        self.assertEqual(inputs['approve_cloud_upload']['default'], 'false')

    def test_only_one_source_and_sensor(self):
        self.assertIn('--source openaq --region HANOI --sensor-id 21632', self.live['run'])
        self.assertIn('--source openaq --region HANOI --sensor-id 21632', self.preview['run'])
        self.assertNotIn('--source era5', self.live['run'])
        self.assertNotIn('--source firms', self.live['run'])

    def test_fixed_reviewed_date(self):
        self.assertEqual(self.doc['jobs']['one-day-openaq']['env']['TICK_AS_OF'], '2024-11-06')
        self.assertIn("assert decisions[0]['item']['start'] == '2024-11-05'", self.preview['run'])

    def test_live_budgets(self):
        line = self.live['run']
        for phrase in ('--max-candidates 1', '--max-http-requests 3',
                       '--max-bytes 2097152', '--max-cloud-bytes 4194304'):
            self.assertIn(phrase, line)

    def test_source_and_cloud_approvals_same_step(self):
        self.assertIn('--approve-live --approve-upload', self.live['run'])
        self.assertNotIn('--approve-live', self.preview['run'])
        self.assertNotIn('--approve-upload', self.preview['run'])

    def test_credentials_not_in_preview(self):
        self.assertNotIn('OPENAQ_API_KEY', self.preview.get('env', {}))
        self.assertNotIn('HF_WRITE_TOKEN', self.preview.get('env', {}))
        self.assertIn('HF_WRITE_TOKEN', self.live['env']['HF_TOKEN'])
        self.assertIn('OPENAQ_API_KEY', self.live['env']['OPENAQ_API_KEY'])

    def test_limited_restore_and_private(self):
        step = next(s for s in self.steps if s.get('name', '').startswith('Restore previously'))
        self.assertIn('HF_READ_TOKEN', step['env']['HF_TOKEN'])
        self.assertIn('--approve-download --max-bytes 8388608', step['run'])
        self.assertIn('CourseworkBigData/env-event-lake', step['run'])

    def test_single_writer_same_group_as_cloud_tick(self):
        old = yaml.load((ROOT / '.github/workflows/ingestion-cloud-tick.yml').read_text(), Loader=yaml.BaseLoader)
        self.assertEqual(self.doc['concurrency']['group'], old['concurrency']['group'])
        self.assertEqual(self.doc['concurrency']['cancel-in-progress'], 'false')

    def test_write_step_refuses_missing_secrets(self):
        self.assertIn('if [[ -z "$HF_TOKEN" || -z "$OPENAQ_API_KEY" ]]', self.live['run'])

    def test_repo_permissions_read_only(self):
        self.assertEqual(self.doc['permissions'], {'contents': 'read'})

    def test_read_only_push_never_invokes_live(self):
        self.assertIn("github.event_name == 'workflow_dispatch'", self.live['if'])
        self.assertNotIn('schedule:', self.text)


if __name__ == '__main__':
    unittest.main()
