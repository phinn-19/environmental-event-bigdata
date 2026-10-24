"""Static CI guardrails for Sprint 11C-A: no unattended source/provider calls."""
from pathlib import Path
import unittest

import yaml


ROOT = Path(__file__).resolve().parents[1]
WORKFLOW = ROOT / '.github' / 'workflows' / 'ingestion-cloud-tick.yml'


class CloudTickCITests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.content = WORKFLOW.read_text(encoding='utf-8')
        cls.wf = yaml.safe_load(cls.content)
        # PyYAML uses YAML 1.1 and resolves 'on' as True; GitHub does not.
        cls.events = cls.wf.get('on', cls.wf.get(True))
        cls.steps = cls.wf['jobs']['tick']['steps']

    def test_push_only_develop(self):
        self.assertEqual(self.events['push']['branches'], ['develop'])

    def test_no_schedule(self):
        self.assertNotIn('schedule', self.events)

    def test_dispatch_has_only_two_modes(self):
        inputs = self.events['workflow_dispatch']['inputs']
        self.assertEqual(inputs['mode']['options'], ['preview', 'publish_existing'])
        self.assertEqual(inputs['mode']['default'], 'preview')

    def test_publish_confirmation_defaults_false(self):
        self.assertIs(self.events['workflow_dispatch']['inputs']
                      ['approve_publish_existing']['default'], False)

    def test_readonly_github_permissions(self):
        self.assertEqual(self.wf['permissions'], {'contents': 'read'})

    def test_non_canceling_single_writer(self):
        c = self.wf['concurrency']
        self.assertEqual(c['group'], 'environmental-event-cloud-tick-single-writer')
        self.assertIs(c['cancel-in-progress'], False)

    def test_no_source_live_flag_in_commands(self):
        commands = '\n'.join(s.get('run', '') for s in self.steps)
        self.assertNotIn('--approve-live', commands)
        self.assertNotIn('-m src.ingestion.daily_execute', commands)

    def test_restore_only_with_read_secret(self):
        step = next(s for s in self.steps if s['name'].startswith('Restore verified'))
        self.assertEqual(step['env']['HF_TOKEN'], '${{ secrets.HF_READ_TOKEN }}')
        self.assertIn('--approve-download', step['run'])

    def test_publish_only_with_write_secret_and_explicit_mode(self):
        step = next(s for s in self.steps if s['name'].startswith('Explicitly authorized'))
        self.assertEqual(step['env']['HF_TOKEN'], '${{ secrets.HF_WRITE_TOKEN }}')
        self.assertIn("inputs.mode == 'publish_existing'", step['if'])
        self.assertIn('inputs.approve_publish_existing == true', step['if'])
        self.assertIn('--approve-upload', step['run'])
        self.assertNotIn('--approve-live', step['run'])

    def test_publish_rejects_pending_days(self):
        step = next(s for s in self.steps if s['name'].startswith('Explicitly authorized'))
        self.assertIn('LOCAL COMPLETE: 1; PENDING: 0; ACQUIRED THIS RUN: 0', step['run'])

    def test_restore_happens_before_preview_and_publish(self):
        names = [s['name'] for s in self.steps]
        restore = next(i for i, n in enumerate(names) if n.startswith('Restore verified'))
        preview = next(i for i, n in enumerate(names) if n.startswith('Offline preview'))
        publish = next(i for i, n in enumerate(names) if n.startswith('Explicitly authorized'))
        self.assertLess(restore, preview)
        self.assertLess(preview, publish)

    def test_cloud_budget_for_preview_and_publish(self):
        for step in self.steps:
            if step['name'].startswith(('Offline preview', 'Explicitly authorized')):
                self.assertIn('--max-cloud-bytes 1048576', step['run'])
                self.assertIn('--max-candidates 1', step['run'])


if __name__ == '__main__':
    unittest.main()
