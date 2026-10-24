"""Contract and safety regression for Sprint 14 multi-source live."""
import unittest
from datetime import date
from pathlib import Path
from unittest.mock import Mock

import yaml
from src.ingestion.daily_live_tick import (
    SOURCES, commands_for, require_credentials, run_tick, source_names,
)

WF = Path(__file__).resolve().parents[1] / '.github/workflows/ingestion-multisource-live.yml'


class MultiSourceLiveTests(unittest.TestCase):
    def test_explicit_reviewed_sources_and_lags(self):
        self.assertEqual(source_names('all'), ('openaq', 'firms', 'era5'))
        self.assertEqual(SOURCES['openaq'].sensor_id, 13502154)
        self.assertEqual(SOURCES['openaq'].region, 'HANOI')
        self.assertEqual(SOURCES['firms'].region, 'VN_BBOX')
        self.assertEqual(SOURCES['firms'].product, 'VIIRS_NOAA20_NRT')
        self.assertEqual(SOURCES['era5'].lag, 7)
        self.assertEqual(SOURCES['openaq'].lag, 2)
        self.assertEqual(SOURCES['firms'].lag, 2)

    def test_utc_one_day_scoped_restore_and_budgets(self):
        for name in SOURCES:
            cmds = commands_for(name, date(2026, 10, 25), 'live')
            self.assertEqual(len(cmds), 3)
            self.assertIn('--scope-day', cmds[0])
            target = '2026-10-18' if name == 'era5' else '2026-10-23'
            self.assertEqual(cmds[0][cmds[0].index('--scope-day') + 1], target)
            for cmd in cmds[1:]:
                self.assertEqual(cmd[cmd.index('--lookback-days') + 1], '1')
                self.assertEqual(cmd[cmd.index('--max-candidates') + 1], '1')
            self.assertNotIn('--approve-live', cmds[1])
            self.assertIn('--approve-live', cmds[2])
            self.assertIn('--approve-upload', cmds[2])

    def test_no_calls_without_two_live_approvals(self):
        run = Mock()
        with self.assertRaises(ValueError):
            run_tick(selected='all', mode='live', approve_api=True,
                     approve_cloud=False, env={}, runner=run)
        run.assert_not_called()

    def test_credentials_checked_before_any_network(self):
        run = Mock()
        with self.assertRaises(ValueError):
            run_tick(selected='all', mode='live', approve_api=True,
                     approve_cloud=True, env={'HF_READ_TOKEN': 'x', 'HF_WRITE_TOKEN': 'y'}, runner=run)
        run.assert_not_called()
        require_credentials('offline', 'all', {})

    def test_offline_does_not_read_tokens(self):
        run = Mock()
        run_tick(selected='all', mode='offline', approve_api=False,
                 approve_cloud=False, now=date(2026, 10, 25),
                 env={'HF_TOKEN': 'sensitive', 'OPENAQ_API_KEY': 'sensitive'}, runner=run)
        self.assertEqual(run.call_count, 3)
        for args, kwargs in run.call_args_list:
            self.assertNotIn('--approve-live', args[0])
            self.assertNotIn('HF_TOKEN', kwargs['env'])
            self.assertNotIn('OPENAQ_API_KEY', kwargs['env'])

    def test_live_uses_read_token_then_no_token_then_write_token(self):
        run = Mock()
        env = {'HF_READ_TOKEN': 'r', 'HF_WRITE_TOKEN': 'w', 'OPENAQ_API_KEY': 'a'}
        run_tick(selected='openaq', mode='live', approve_api=True,
                 approve_cloud=True, now=date(2026, 10, 25), env=env, runner=run)
        self.assertEqual(run.call_count, 3)
        restore_env = run.call_args_list[0].kwargs['env']
        self.assertEqual(restore_env['HF_TOKEN'], 'r')
        self.assertNotIn('HF_WRITE_TOKEN', restore_env)
        self.assertNotIn('OPENAQ_API_KEY', restore_env)
        self.assertNotIn('HF_TOKEN', run.call_args_list[1].kwargs['env'])
        live_env = run.call_args_list[2].kwargs['env']
        self.assertEqual(live_env['HF_TOKEN'], 'w')
        self.assertEqual(live_env['OPENAQ_API_KEY'], 'a')
        self.assertNotIn('HF_READ_TOKEN', live_env)

    def test_workflow_only_explicit_dispatch_can_run_live(self):
        text = WF.read_text(encoding='utf-8')
        doc = yaml.load(text, Loader=yaml.BaseLoader)
        self.assertEqual(set(doc['on']), {'push', 'workflow_dispatch'})
        self.assertEqual(doc['permissions'], {'contents': 'read'})
        self.assertEqual(doc['concurrency']['group'], 'environmental-event-cloud-tick-single-writer')
        self.assertIn("github.ref == 'refs/heads/develop'", doc['jobs']['bounded-tick']['if'])
        inputs = doc['on']['workflow_dispatch']['inputs']
        self.assertEqual(inputs['mode']['default'], 'preview')
        self.assertEqual(inputs['approve_source_api']['default'], 'false')
        self.assertEqual(inputs['approve_cloud_upload']['default'], 'false')
        steps = doc['jobs']['bounded-tick']['steps']
        live = [step for step in steps if step['name'].startswith('Explicitly approved')]
        self.assertEqual(len(live), 1)
        for condition in ('github.event_name == \'workflow_dispatch\'',
                          'inputs.mode == \'live_acquire\'',
                          'inputs.approve_source_api == true',
                          'inputs.approve_cloud_upload == true'):
            self.assertIn(condition, live[0]['if'])
        self.assertIn('--approve-api --approve-cloud', live[0]['run'])
        for step in steps:
            if step is not live[0]:
                self.assertNotIn('--approve-api --approve-cloud', step.get('run', ''))
        self.assertNotIn('schedule:', text)
        self.assertNotIn('cron:', text)
        self.assertNotIn('secrets.HF_WRITE_TOKEN', text.split('- name: Explicitly approved')[0])


if __name__ == '__main__':
    unittest.main()
