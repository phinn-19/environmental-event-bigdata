"""Fail-closed, offline tests for the incremental coordinator."""
from __future__ import annotations

import io
import unittest
from contextlib import redirect_stdout
from pathlib import Path
from tempfile import TemporaryDirectory
from unittest.mock import patch

from src.ingestion.backfill_audit import AuditResult
from src.ingestion.daily_execute import execute_daily, main


class TestDailyExecute(unittest.TestCase):
    def setUp(self):
        self.tmp = TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.root = Path(self.tmp.name)
        self.cfg = {'regions': {'HANOI': {}}}
        self.base = dict(root=self.root, config=self.cfg, source='era5', region_id='HANOI',
                         as_of='2024-11-05', lag_days=1, lookback_days=2, max_candidates=2)
        self.status = {}
        self.calls = []

    def audit(self, *, root, item, config):
        status = self.status.get(item.start, 'not_found')
        return AuditResult(item, status, 'fake_audit')

    def dispatch(self, *, root, item, config, max_bytes, max_http_requests):
        self.calls.append(item.start)
        self.status[item.start] = 'complete'
        return 'published'

    def run_it(self, **kwargs):
        return execute_daily(**(self.base | dict(auditor=self.audit,
                                                  executor=self.dispatch) | kwargs))

    def test_dry_run_never_creates_lock_or_dispatches(self):
        r = self.run_it(approve_live=False)
        self.assertEqual(r.state, 'approval_required')
        self.assertEqual(r.candidates, 2)
        self.assertFalse((self.root / 'data').exists())
        self.assertEqual(self.calls, [])

    def test_hold_blocks_entire_batch(self):
        self.status['2024-11-03'] = 'review'
        r = self.run_it(approve_live=True)
        self.assertEqual(r.state, 'blocked_review')
        self.assertEqual(self.calls, [])
        self.assertFalse((self.root / 'data').exists())

    def test_budget_blocks_entire_batch(self):
        r = self.run_it(approve_live=True, max_candidates=1)
        self.assertEqual(r.state, 'blocked_budget')
        self.assertEqual(self.calls, [])

    def test_success_then_idempotent_noop(self):
        r = self.run_it(approve_live=True)
        self.assertEqual(r.state, 'published')
        self.assertEqual(r.published, 2)
        self.assertEqual(self.calls, ['2024-11-03', '2024-11-04'])
        self.assertFalse((self.root / 'data/raw/_locks/daily_ingestion.lock').exists())
        r2 = self.run_it(approve_live=True)
        self.assertEqual(r2.state, 'nothing_to_do')
        self.assertEqual(len(self.calls), 2)

    def test_second_request_failure_stops_and_can_resume(self):
        def fail_on_second(**args):
            item = args['item']
            self.calls.append(item.start)
            if item.start == '2024-11-04':
                return 'failed'
            self.status[item.start] = 'complete'
            return 'published'
        r = self.run_it(approve_live=True, executor=fail_on_second)
        self.assertEqual(r.state, 'failed')
        self.assertEqual(r.published, 1)
        self.assertEqual(len(self.calls), 2)
        r2 = self.run_it(approve_live=True)
        self.assertEqual(r2.state, 'published')
        self.assertEqual(r2.skipped, 1)
        self.assertEqual(r2.published, 1)

    def test_post_publish_audit_failure_fails_closed(self):
        def publish_without_data(**args):
            self.calls.append(args['item'].start)
            return 'published'
        r = self.run_it(approve_live=True, executor=publish_without_data)
        self.assertEqual(r.reason, 'post_publish_audit_not_complete')
        self.assertEqual(len(self.calls), 1)

    def test_global_lock_blocks_parallel_or_stale_writer(self):
        p = self.root / 'data/raw/_locks/daily_ingestion.lock'
        p.parent.mkdir(parents=True)
        p.write_text('stale\n')
        r = self.run_it(approve_live=True)
        self.assertEqual(r.state, 'blocked_review')
        self.assertTrue(p.exists())
        self.assertEqual(self.calls, [])

    def test_plan_change_after_lock_blocks_without_dispatch(self):
        real = __import__('src.ingestion.daily_execute', fromlist=['evaluate_gate'])
        original = real.evaluate_gate
        seen = [0]
        def change(*args, **kwargs):
            seen[0] += 1
            ret = original(*args, **kwargs)
            if seen[0] == 2:
                self.status['2024-11-03'] = 'review'
                return original(*args, **kwargs)
            return ret
        with patch.object(real, 'evaluate_gate', side_effect=change):
            r = self.run_it(approve_live=True)
        self.assertEqual(r.reason, 'plan_changed_after_lock')
        self.assertEqual(self.calls, [])

    def test_invalid_limits_rejected_without_data_mutation(self):
        for fields in ({'max_candidates': 4}, {'max_http_requests': 0},
                       {'max_bytes': 0}, {'max_bytes': 16 * 1024 * 1024 + 1}):
            with self.subTest(fields=fields), self.assertRaises(ValueError):
                self.run_it(approve_live=True, **fields)
        self.assertFalse((self.root / 'data').exists())

    def test_offline_cli_never_creates_files(self):
        stdout = io.StringIO()
        with redirect_stdout(stdout):
            code = main(['--source', 'era5', '--region', 'HANOI',
                         '--as-of', '2020-01-03', '--lag-days', '1',
                         '--lookback-days', '1'])
        self.assertEqual(code, 0)
        self.assertIn('DRY RUN ONLY', stdout.getvalue())


if __name__ == '__main__':
    unittest.main()
