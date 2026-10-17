"""Actual OpenAQ adapter tested end-to-end with mock transport, no HTTP or live creds."""
from __future__ import annotations

import json
import tempfile
import unittest
import urllib.error
from datetime import date
from pathlib import Path
from unittest.mock import patch

from src.ingestion.backfill import PlanItem
from src.ingestion.backfill_audit import audit_item
from src.ingestion.backfill_live import execute_one_openaq_day, main
from src.ingestion.openaq import _paths

CONFIG = {
    'regions': {'HANOI': {'north': 21.25, 'west': 105.5, 'south': 20.75, 'east': 106.0}},
    'ingestion': {'openaq': {'page_limit': 100, 'pilot_sensor_ids': [21632]}},
}
ITEM = PlanItem('openaq', 'HANOI', '2024-12-15', '2024-12-16', sensor_id=21632)


def payload(n=1, found=1, day='2024-12-15'):
    obj = {'meta': {'found': found},
           'results': [{'period': {'datetimeFrom': {'utc': f'{day}T00:00:00Z'}},
                        'value': 15.0} for _ in range(n)]}
    return json.dumps(obj).encode('utf-8')


class GuardedLiveTests(unittest.TestCase):
    def setUp(self):
        temp = tempfile.TemporaryDirectory()
        self.addCleanup(temp.cleanup)
        self.root = Path(temp.name)
        self.called = 0

    def fake(self, url, headers, timeout):
        self.called += 1
        return payload()

    def runit(self, **overrides):
        args = dict(root=self.root, item=ITEM, config=CONFIG, approve_live=True,
                    api_key='test-key-only', max_http_requests=2,
                    max_bytes=2048, transport=self.fake, sleep=lambda _: None)
        args.update(overrides)
        return execute_one_openaq_day(**args)

    def test_mock_adapter_publishes_real_raw_and_manifest(self):
        result = self.runit()
        self.assertEqual(result.state, 'published')
        self.assertEqual(result.requests, 1)
        self.assertEqual(result.pages, 1)
        self.assertEqual(self.called, 1)
        raw, mf = _paths(self.root, 21632, date(2024, 12, 15), 1)
        self.assertEqual(raw.read_bytes(), payload())
        manifest = json.loads(mf.read_text(encoding='utf-8'))
        self.assertEqual(manifest['acquisition_mode'], 'live')
        self.assertEqual(manifest['row_count'], 1)
        self.assertEqual(audit_item(root=self.root, item=ITEM, config=CONFIG).status, 'complete')
        self.assertEqual(self.runit().state, 'skipped')
        self.assertEqual(self.called, 1)

    def test_dry_run_does_not_touch_files_or_network(self):
        r = self.runit(approve_live=False, api_key=None)
        self.assertEqual(r.state, 'approval_required')
        self.assertEqual(self.called, 0)
        self.assertFalse((self.root / 'data').exists())

    def test_existing_replay_provenance_blocks_before_http(self):
        raw, mf = _paths(self.root, 21632, date(2024, 12, 15), 1)
        raw.parent.mkdir(parents=True, exist_ok=True)
        mf.parent.mkdir(parents=True, exist_ok=True)
        raw.write_bytes(payload())
        mf.write_text('{"acquisition_mode":"replay"}', encoding='utf-8')
        before = raw.read_bytes(), mf.read_bytes()
        result = self.runit()
        self.assertEqual(result.state, 'blocked_review')
        self.assertEqual(self.called, 0)
        self.assertEqual(before, (raw.read_bytes(), mf.read_bytes()))

    def test_staged_invalid_json_never_published(self):
        r = self.runit(transport=lambda *_: b'{invalid')
        self.assertEqual(r.state, 'failed')
        self.assertEqual(audit_item(root=self.root, item=ITEM, config=CONFIG).status, 'not_found')

    def test_staged_timeout_never_published(self):
        def broken(*_):
            raise urllib.error.URLError('network')
        r = self.runit(transport=broken)
        self.assertEqual(r.state, 'failed')
        self.assertEqual(audit_item(root=self.root, item=ITEM, config=CONFIG).status, 'not_found')

    def test_byte_budget_blocks_publication(self):
        r = self.runit(max_bytes=10)
        self.assertEqual(r.state, 'failed')
        self.assertEqual(self.called, 1)
        self.assertEqual(audit_item(root=self.root, item=ITEM, config=CONFIG).status, 'not_found')

    def test_request_budget_blocks_second_page(self):
        self.assertEqual(CONFIG['ingestion']['openaq']['page_limit'], 100)
        def full_page(*_):
            self.called += 1
            return payload(n=100, found=150)
        r = self.runit(max_http_requests=1, max_bytes=20000, transport=full_page)
        self.assertEqual(r.state, 'failed')
        self.assertEqual(self.called, 1)
        self.assertEqual(audit_item(root=self.root, item=ITEM, config=CONFIG).status, 'not_found')

    def test_two_pages_published_together(self):
        pages = iter((payload(n=100, found=101), payload(n=1, found=101)))
        def two(*_):
            self.called += 1
            return next(pages)
        r = self.runit(max_bytes=30000, transport=two)
        self.assertEqual(r.state, 'published')
        self.assertEqual(r.requests, 2)
        self.assertEqual(r.pages, 2)
        self.assertEqual(audit_item(root=self.root, item=ITEM, config=CONFIG).status, 'complete')

    def test_lock_blocks_second_writer_before_http(self):
        lock = self.root / 'data/raw/openaq/_locks/sensor=21632-day=2024-12-15.lock'
        lock.parent.mkdir(parents=True, exist_ok=True)
        lock.write_text('simulated other writer')
        r = self.runit()
        self.assertEqual(r.state, 'blocked_review')
        self.assertEqual(r.reason, 'active_or_stale_lock')
        self.assertEqual(self.called, 0)
        self.assertTrue(lock.exists())

    def test_preflight_change_after_lock_aborts_before_http(self):
        from src.ingestion.backfill_audit import AuditResult
        original = audit_item
        checks = 0
        def drifting(*, root, item, config):
            nonlocal checks
            checks += 1
            if checks == 1:
                return AuditResult(item, 'review', 'changed')
            return original(root=root, item=item, config=config)
        with patch('src.ingestion.backfill_live.audit_item', side_effect=drifting):
            r = self.runit()
        self.assertEqual(r.state, 'blocked_review')
        self.assertEqual(self.called, 0)

    def test_external_writer_during_stage_prevents_publish(self):
        day = date(2024, 12, 15)
        dest, mf = _paths(self.root, 21632, day, 1)
        def concurrent(*_):
            # Simulate another process creating a replay snapshot mid-download.
            dest.parent.mkdir(parents=True, exist_ok=True)
            mf.parent.mkdir(parents=True, exist_ok=True)
            dest.write_bytes(b'external_raw')
            mf.write_text('{"acquisition_mode":"replay"}', encoding='utf-8')
            return payload()
        r = self.runit(transport=concurrent)
        self.assertEqual(r.state, 'blocked_review')
        self.assertEqual(dest.read_bytes(), b'external_raw')
        self.assertEqual(json.loads(mf.read_text())['acquisition_mode'], 'replay')

    def test_publication_failure_rolls_back_new_files(self):
        def disk_full(*_args, **_kwargs):
            raise OSError('simulate full disk')
        with patch('src.ingestion.backfill_live.shutil.copyfileobj', side_effect=disk_full):
            r = self.runit()
        self.assertEqual(r.state, 'failed')
        self.assertEqual(r.reason, 'publish_failed')
        self.assertEqual(audit_item(root=self.root, item=ITEM, config=CONFIG).status, 'not_found')

    def test_real_http_response_is_read_with_bound(self):
        class FakeResponse:
            status = 200
            def __init__(self):
                self.maxread = None
            def __enter__(self):
                return self
            def __exit__(self, *_):
                return False
            def read(self, maxbytes):
                self.maxread = maxbytes
                return b'x' * maxbytes
        response = FakeResponse()
        class FakeOpener:
            def open(self, _request, timeout):
                return response
        with patch('src.ingestion.backfill_live.urllib.request.build_opener',
                   return_value=FakeOpener()):
            r = self.runit(transport=None, max_bytes=16)
        self.assertEqual(r.state, 'failed')
        self.assertEqual(r.requests, 1)
        self.assertEqual(response.maxread, 17)
        self.assertEqual(audit_item(root=self.root, item=ITEM, config=CONFIG).status, 'not_found')

    def test_no_implicit_execution_on_cli(self):
        with patch('src.ingestion.backfill_live.ROOT', self.root), \
             patch('src.ingestion.backfill_live.validate_config', return_value=CONFIG), \
             patch('src.ingestion.backfill_live.execute_one_openaq_day', wraps=execute_one_openaq_day):
            self.assertEqual(main(['--region', 'HANOI', '--sensor-id', '21632',
                                   '--start', '2024-12-15', '--end', '2024-12-16']), 0)
        self.assertFalse((self.root / 'data').exists())

    def test_reject_multiday_and_foreign_source(self):
        with self.assertRaises(ValueError):
            self.runit(item=PlanItem('openaq','HANOI','2024-12-15','2024-12-17',sensor_id=21632))
        with self.assertRaises(ValueError):
            self.runit(item=PlanItem('firms','HANOI','2024-12-15','2024-12-16', product='VIIRS_NOAA20_SP'))
        self.assertEqual(self.called, 0)

    def test_reject_bad_budgets(self):
        for kwargs in ({'max_bytes': 0}, {'max_http_requests': 0},
                       {'max_http_requests': 100}, {'max_bytes': 50*1024*1024}):
            with self.assertRaises(ValueError):
                self.runit(**kwargs)
        self.assertEqual(self.called, 0)


if __name__ == '__main__':
    unittest.main()
