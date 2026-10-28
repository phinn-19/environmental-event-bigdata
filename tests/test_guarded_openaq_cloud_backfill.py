import importlib.util
import os
from datetime import date
from pathlib import Path
import unittest

SCRIPT = Path(__file__).resolve().parents[1] / 'scripts' / 'bulk' / 'openaq_cloud_backfill.py'
spec = importlib.util.spec_from_file_location('openaq_cloud_backfill', SCRIPT)
module = importlib.util.module_from_spec(spec)
spec.loader.exec_module(module)


class GuardedBatchTests(unittest.TestCase):
    def setUp(self):
        self.today = date(2026, 10, 28)
        self.calls = []
        self.env = {'OPENAQ_REDISTRIBUTION_APPROVED': 'true',
                    'HF_READ_TOKEN': 'read-secret', 'HF_WRITE_TOKEN': 'write-secret',
                    'OPENAQ_API_KEY': 'provider-secret'}

    def runner(self, args, *, check, env):
        self.assertTrue(check)
        self.calls.append((args, env))

    def test_plan_no_side_effects(self):
        module.run(start='2026-07-01', days=7, mode='plan', approve_api=False,
                   approve_cloud=False, runner=self.runner, today=self.today)
        self.assertEqual(self.calls, [])

    def test_day_and_future_limits(self):
        for start, days in [('2026-07-01', 8), ('2026-10-28', 1),
                            ('2024-01-01', 0), ('2026-02-30', 1)]:
            with self.subTest(start=start, days=days), self.assertRaises(ValueError):
                module.plan_days(start, days, today=self.today)

    def test_live_requires_clearance_and_both_approvals(self):
        for missing in ('HF_READ_TOKEN','HF_WRITE_TOKEN','OPENAQ_API_KEY','OPENAQ_REDISTRIBUTION_APPROVED'):
            env = dict(self.env)
            del env[missing]
            with self.subTest(missing=missing), self.assertRaises(ValueError):
                module.run(start='2026-07-01', days=1, mode='live', approve_api=True,
                           approve_cloud=True, env=env, runner=self.runner, today=self.today)
        with self.assertRaises(ValueError):
            module.run(start='2026-07-01', days=1, mode='live', approve_api=True,
                       approve_cloud=False, env=self.env, runner=self.runner, today=self.today)
        self.assertEqual(self.calls, [])

    def test_preview_is_offline_without_secrets(self):
        module.run(start='2026-07-01', days=2, mode='preview', approve_api=False,
                   approve_cloud=False, env=self.env, runner=self.runner, today=self.today)
        self.assertEqual(len(self.calls), 2)
        for args, env in self.calls:
            self.assertNotIn('--approve-live', args)
            for k in ('HF_TOKEN', 'OPENAQ_API_KEY', 'HF_WRITE_TOKEN'):
                self.assertNotIn(k, env)

    def test_live_restores_each_day_then_publishes(self):
        module.run(start='2026-07-01', days=3, mode='live', approve_api=True,
                   approve_cloud=True, env=self.env, runner=self.runner, today=self.today)
        self.assertEqual(len(self.calls), 6)
        for i in range(0, 6, 2):
            restore, read_env = self.calls[i]
            live, write_env = self.calls[i+1]
            self.assertIn('src.ingestion.cloud_restore', restore)
            self.assertEqual(read_env.get('HF_TOKEN'), 'read-secret')
            self.assertNotIn('OPENAQ_API_KEY', read_env)
            self.assertIn('src.ingestion.daily_cloud_workflow', live)
            self.assertIn('--approve-live', live)
            self.assertIn('--approve-upload', live)
            self.assertEqual(write_env.get('HF_TOKEN'), 'write-secret')
            self.assertNotIn('HF_READ_TOKEN', write_env)

if __name__ == '__main__':
    unittest.main()
