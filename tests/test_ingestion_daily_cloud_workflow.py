"""Safe composition tests: Sprint 08 guarded executor + Sprint 09 HF Publisher."""
from __future__ import annotations

import io
import unittest
from contextlib import redirect_stdout
from pathlib import Path
from tempfile import TemporaryDirectory
from unittest.mock import patch

from src.ingestion.backfill_audit import AuditResult
from src.ingestion.cloud_publish import Entry
from src.ingestion.daily_execute import DailyExecutionResult
from src.ingestion.daily_cloud_workflow import main, run_workflow


class DailyCloudWorkflowTests(unittest.TestCase):
    def setUp(self):
        self.tmp = TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.root = Path(self.tmp.name)
        self.config = {'regions': {'HANOI': {}}}
        self.status = {}
        self.executor_calls = 0
        self.upload_calls = 0
        self.upload_error = False
        self.base = dict(
            root=self.root, config=self.config, source='era5', region='HANOI',
            as_of='2024-11-06', lag_days=1, lookback_days=1,
            max_candidates=1, auditor=self.audit, executor=self.execute,
            entry_builder=self.prepare, uploader=self.upload,
        )

    def audit(self, *, root, item, config):
        return AuditResult(item, self.status.get(item.start, 'not_found'), 'mock')

    def execute(self, **kwargs):
        self.executor_calls += 1
        self.status['2024-11-05'] = 'complete'
        return DailyExecutionResult('published', 1, 1, 0, 'mock')

    def prepare(self, *, start, prefix, **kwargs):
        return (
            Entry(self.root / 'raw', f'{prefix}/data/raw/{start}.grib', 9, 'a' * 64, False),
            Entry(self.root / 'manifest', f'{prefix}/data/raw/{start}.manifest.json', 5,
                  'b' * 64, True),
        )

    def upload(self, bucket, entries):
        self.upload_calls += 1
        if self.upload_error:
            raise OSError('simulated cloud outage')
        self.assertEqual(len(entries), 2)
        self.assertFalse(entries[0].is_manifest)
        self.assertTrue(entries[1].is_manifest)
        return 2, 0

    def run_it(self, **kwargs):
        return run_workflow(**(self.base | kwargs))

    def test_preview_missing_is_offline_no_writer(self):
        result = self.run_it()
        self.assertEqual((result.state, result.candidates), ('preview', 1))
        self.assertEqual((self.executor_calls, self.upload_calls), (0, 0))
        self.assertFalse((self.root / 'data').exists())

    def test_preview_existing_shows_objects_without_cloud_calls(self):
        self.status['2024-11-05'] = 'complete'
        result = self.run_it()
        self.assertEqual((result.state, result.objects, result.bytes_ready),
                         ('preview', 2, 14))
        self.assertEqual((self.executor_calls, self.upload_calls), (0, 0))

    def test_live_requires_both_approvals_before_provider(self):
        for approvals in ({'approve_live': True}, {'approve_upload': True}):
            with self.subTest(approvals=approvals):
                result = self.run_it(**approvals)
                self.assertEqual(result.state, 'approval_required')
        self.assertEqual((self.executor_calls, self.upload_calls), (0, 0))

    def test_live_download_then_cloud_verify(self):
        result = self.run_it(approve_live=True, approve_upload=True)
        self.assertEqual(result.state, 'published')
        self.assertEqual((result.acquired, result.cloud_new), (1, 2))
        self.assertEqual((self.executor_calls, self.upload_calls), (1, 1))

    def test_cloud_only_backlog_does_not_call_provider(self):
        self.status['2024-11-05'] = 'complete'
        result = self.run_it(approve_upload=True)
        self.assertEqual(result.state, 'published')
        self.assertEqual(result.acquired, 0)
        self.assertEqual((self.executor_calls, self.upload_calls), (0, 1))

    def test_review_and_budget_block_before_side_effects(self):
        self.status['2024-11-05'] = 'review'
        result = self.run_it(approve_live=True, approve_upload=True)
        self.assertEqual(result.state, 'blocked_review')
        self.status.clear()
        result = self.run_it(approve_live=True, approve_upload=True,
                             lookback_days=2)
        self.assertEqual(result.state, 'blocked_budget')
        self.assertEqual((self.executor_calls, self.upload_calls), (0, 0))

    def test_guarded_executor_failure_never_uploads_partial_batch(self):
        def fail(**kwargs):
            self.executor_calls += 1
            self.status['2024-11-04'] = 'complete'
            return DailyExecutionResult('failed', 2, 1, 0, 'source_failed')
        result = self.run_it(lookback_days=2, max_candidates=2,
                             executor=fail, approve_live=True, approve_upload=True)
        self.assertEqual(result.state, 'failed')
        self.assertEqual(result.acquired, 1)
        self.assertEqual(self.upload_calls, 0)

    def test_post_execution_audit_not_complete_never_uploads(self):
        def incomplete(**kwargs):
            self.executor_calls += 1
            return DailyExecutionResult('published', 1, 1, 0, 'mock')
        result = self.run_it(executor=incomplete, approve_live=True, approve_upload=True)
        self.assertEqual(result.state, 'blocked_review')
        self.assertEqual(self.upload_calls, 0)

    def test_remote_failure_preserves_local_and_cloud_only_retry(self):
        self.status['2024-11-05'] = 'complete'
        self.upload_error = True
        with self.assertRaises(OSError):
            self.run_it(approve_upload=True)
        self.assertEqual(self.executor_calls, 0)
        self.upload_error = False
        result = self.run_it(approve_upload=True)
        self.assertEqual(result.state, 'published')
        self.assertEqual(self.executor_calls, 0)

    def test_cloud_byte_budget_before_upload(self):
        self.status['2024-11-05'] = 'complete'
        with self.assertRaisesRegex(ValueError, 'budget exceeded'):
            self.run_it(approve_upload=True, max_cloud_bytes=13)
        self.assertEqual(self.upload_calls, 0)

    def test_invalid_cloud_destination_and_limits_reject_early(self):
        for invalid in ({'prefix': '../unsafe'}, {'bucket': 'bad bucket'},
                        {'max_candidates': 4}, {'max_cloud_bytes': 0},
                        {'max_bytes': -1}):
            with self.subTest(invalid=invalid), self.assertRaises(ValueError):
                self.run_it(approve_live=True, approve_upload=True, **invalid)
        self.assertEqual((self.executor_calls, self.upload_calls), (0, 0))

    def test_cli_real_preview_stays_offline(self):
        # 2020 window is unpopulated in the repo snapshot. Never access providers.
        text = io.StringIO()
        with redirect_stdout(text):
            code = main(['--source', 'era5', '--region', 'HANOI',
                         '--as-of', '2020-01-03', '--lag-days', '1',
                         '--lookback-days', '1'])
        self.assertEqual(code, 0)
        self.assertIn('DRY RUN ONLY', text.getvalue())


if __name__ == '__main__':
    unittest.main()
