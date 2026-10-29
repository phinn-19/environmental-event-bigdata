import unittest
from datetime import date
from scripts.bulk.era5_cloud_batch import execute, scheduled_days


class ERA5BatchTests(unittest.TestCase):
    def test_scheduled_start(self):
        self.assertEqual(scheduled_days(today=date(2026, 10, 30)),
                         (date(2024, 11, 13), date(2024, 11, 14), date(2024, 11, 15)))

    def test_no_early_campaign(self):
        self.assertEqual(scheduled_days(today=date(2026, 10, 29)), ())

    def test_limit_past_and_invalid(self):
        self.assertEqual(scheduled_days(today=date(2026, 10, 30),
                                        first=date(2026, 10, 28)), ())
        with self.assertRaises(ValueError):
            scheduled_days(today=date(2026, 10, 30), days=30)

    def test_preview_has_no_network_secret(self):
        commands = []
        def runner(command, **kwargs):
            commands.append((command, kwargs))
        self.assertEqual(execute(today=date(2026, 10, 30), first=None, days=3,
                                 mode='preview', approve_api=False, approve_cloud=False,
                                 env={'HF_WRITE_TOKEN': 'sensitive'}, runner=runner), 0)
        self.assertEqual(len(commands), 3)
        self.assertTrue(all('--approve-live' not in cmd for cmd, _ in commands))
        self.assertTrue(all('HF_WRITE_TOKEN' not in opts['env'] for _, opts in commands))

    def test_live_restore_then_publish_with_scope(self):
        commands = []
        def runner(command, **kwargs):
            commands.append((command, kwargs))
        env = {'HF_READ_TOKEN': 'read', 'HF_WRITE_TOKEN': 'write', 'CDSAPI_KEY': 'cds'}
        execute(today=date(2026, 10, 30), first=None, days=2, mode='live',
                approve_api=True, approve_cloud=True, env=env, runner=runner)
        self.assertEqual(len(commands), 4)
        self.assertEqual(sum('src.ingestion.cloud_restore' in cmd for cmd, _ in commands), 2)
        self.assertEqual(sum('--approve-upload' in cmd for cmd, _ in commands), 2)
        self.assertEqual(commands[0][1]['env']['HF_TOKEN'], 'read')
        self.assertEqual(commands[1][1]['env']['HF_TOKEN'], 'write')

    def test_live_requires_approvals(self):
        with self.assertRaises(ValueError):
            execute(today=date(2026, 10, 30), first=None, days=3, mode='live',
                    approve_api=True, approve_cloud=False,
                    env={'HF_READ_TOKEN':'r', 'HF_WRITE_TOKEN':'w', 'CDSAPI_KEY':'c'})


if __name__ == '__main__':
    unittest.main()
