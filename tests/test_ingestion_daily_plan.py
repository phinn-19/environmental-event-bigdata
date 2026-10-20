"""Regression tests for read-only incremental scheduling preview."""
from __future__ import annotations

import io
import unittest
from contextlib import redirect_stdout
from datetime import date
from pathlib import Path
from tempfile import TemporaryDirectory

from src.ingestion.backfill_audit import AuditResult
from src.ingestion.backfill_gate import evaluate_gate
from src.ingestion.daily_plan import main, plan_daily


class DailyPlanTests(unittest.TestCase):
    def test_openaq_utc_lag_and_end_exclusive(self):
        items = plan_daily(source='openaq', region_id='HANOI',
                           as_of='2026-10-20', lag_days=2,
                           lookback_days=3, sensor_ids=(21632,))
        self.assertEqual([(i.start, i.end) for i in items], [
            ('2026-10-16', '2026-10-17'),
            ('2026-10-17', '2026-10-18'),
            ('2026-10-18', '2026-10-19'),
        ])
        self.assertTrue(all(i.sensor_id == 21632 for i in items))

    def test_firms_uses_stable_one_day_chunks(self):
        items = plan_daily(source='firms', region_id='VN_BBOX',
                           as_of='2026-10-20', lag_days=3,
                           lookback_days=4, product='VIIRS_NOAA20_NRT')
        self.assertEqual([i.start for i in items], [
            '2026-10-14', '2026-10-15', '2026-10-16', '2026-10-17',
        ])
        self.assertTrue(all(i.end > i.start and i.product == 'VIIRS_NOAA20_NRT'
                            for i in items))
        self.assertTrue(all(
            (date.fromisoformat(i.end) - date.fromisoformat(i.start)).days == 1
            for i in items
        ))

    def test_era5_day_count(self):
        items = plan_daily(source='era5', region_id='HANOI',
                           as_of='2026-10-20', lag_days=7, lookback_days=5)
        self.assertEqual(len(items), 5)
        self.assertEqual(items[-1].start, '2026-10-13')

    def test_input_and_budget_rejections(self):
        base = dict(source='era5', region_id='HANOI', as_of='2026-10-20')
        for overrides in ({'lag_days': 0}, {'lag_days': 1, 'lookback_days': 0},
                          {'lag_days': True},
                          {'lag_days': 1, 'lookback_days': 32},
                          {'lag_days': 1, 'lookback_days': 4, 'max_plan_items': 3}):
            with self.subTest(overrides=overrides), self.assertRaises(ValueError):
                plan_daily(**{**base, **overrides})

    def test_review_blocks_entire_batch_even_if_one_missing(self):
        items = plan_daily(source='era5', region_id='HANOI',
                           as_of='2026-10-20', lag_days=1,
                           lookback_days=2)
        def audit(*, root, item, config):
            return AuditResult(item, 'review' if item == items[0] else 'not_found',
                               'offline_test')
        with TemporaryDirectory() as tmp:
            result = evaluate_gate(root=Path(tmp), items=items,
                                   config={}, auditor=audit)
        self.assertEqual(result.state, 'blocked_review')
        self.assertEqual([d.action for d in result.decisions], ['hold', 'candidate'])

    def test_cli_is_offline_and_no_files_written(self):
        # Use the repository config and an old date, but never invoke provider clients.
        buf = io.StringIO()
        with redirect_stdout(buf):
            code = main(['--source', 'era5', '--region', 'HANOI',
                         '--as-of', '2020-01-03', '--lag-days', '1',
                         '--lookback-days', '1'])
        self.assertEqual(code, 0)
        self.assertIn('OFFLINE ONLY', buf.getvalue())
        self.assertIn('NO LIVE REQUESTS AUTHORIZED', buf.getvalue())


if __name__ == '__main__':
    unittest.main()
