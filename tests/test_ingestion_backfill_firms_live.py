"""Guarded FIRMS integration tests use mock transports only; NEVER network."""
from __future__ import annotations

import io
import json
import tempfile
import unittest
import urllib.error
from contextlib import redirect_stdout
from datetime import date
from pathlib import Path
from unittest.mock import patch

from src.ingestion.backfill import PlanItem
from src.ingestion.backfill_audit import audit_item
from src.ingestion.backfill_firms_live import execute_one_firms_window, main
from src.ingestion.firms import VIIRS_COLUMNS, _paths, valid_bbox

CONFIG = {
    'regions': {'VN_BBOX': {'north': 24., 'west': 102., 'south': 8., 'east': 110.}},
    'ingestion': {'firms': {'max_days_per_request': 5}},
}
ITEM = PlanItem('firms', 'VN_BBOX', '2024-11-01', '2024-11-02', product='VIIRS_NOAA20_SP')
HEADER = ('latitude,longitude,bright_ti4,scan,track,acq_date,acq_time,satellite,'
          'instrument,confidence,version,bright_ti5,frp,daynight\n')
ROW = ('11.17867,107.44868,336.78,0.39,0.36,{day},0616,N20,VIIRS,n,'
       '2.0,292.69,3.58,D\n')


def csv_payload(*, day='2024-11-01', empty=False):
    return (HEADER + ('' if empty else ROW.format(day=day))).encode('utf-8')


class GuardedFIRMSTests(unittest.TestCase):
    def setUp(self):
        ctx = tempfile.TemporaryDirectory(prefix='sprint06-firms-test-')
        self.addCleanup(ctx.cleanup)
        self.root = Path(ctx.name)
        self.calls = 0

    def fake(self, url, timeout):
        self.calls += 1
        return csv_payload()

    def execute(self, *, item=ITEM, approve_live=True, api_key='FAKE-KEY',
                max_bytes=4096, transport=None):
        return execute_one_firms_window(
            root=self.root, item=item, config=CONFIG, approve_live=approve_live,
            api_key=api_key, max_bytes=max_bytes,
            transport=self.fake if transport is None else transport,
        )

    def paths(self, item=ITEM):
        begin = date.fromisoformat(item.start)
        days = (date.fromisoformat(item.end) - begin).days
        return _paths(self.root, item.product, item.region_id,
                      begin, days, valid_bbox(CONFIG['regions'][item.region_id]), 'live')

    def test_success_publishes_byte_identical_raw_and_valid_manifest(self):
        result = self.execute()
        self.assertEqual(result.state, 'published')
        self.assertEqual((result.requests, result.chunks, self.calls), (1, 1, 1))
        raw, mf, _ = self.paths()
        self.assertEqual(raw.read_bytes(), csv_payload())
        m = json.loads(mf.read_text(encoding='utf-8'))
        self.assertEqual((m['status'], m['row_count'], m['acquisition_mode']),
                         ('success', 1, 'live'))
        self.assertEqual(audit_item(root=self.root, item=ITEM, config=CONFIG).status,
                         'complete')
        self.assertNotIn('FAKE-KEY', mf.read_text())
        self.assertFalse(list((self.root / 'data/raw/nasa_firms/_locks').glob('*.lock')))

    def test_skip_after_success_does_not_call_http(self):
        self.assertEqual(self.execute().state, 'published')
        def no_call(*args):
            raise AssertionError('skip must not call HTTP')
        result = self.execute(transport=no_call)
        self.assertEqual(result.state, 'skipped')
        self.assertEqual(result.requests, 0)
        self.assertEqual(self.calls, 1)

    def test_no_approval_creates_no_files_and_calls_no_http(self):
        result = self.execute(approve_live=False, api_key=None)
        self.assertEqual((result.state, result.requests), ('approval_required', 0))
        self.assertEqual(list(self.root.rglob('*')), [])
        self.assertEqual(self.calls, 0)

    def test_invalid_csv_causes_no_publication(self):
        result = self.execute(transport=lambda *_: b'<html>rate limit</html>')
        self.assertEqual(result.state, 'failed')
        self.assertEqual(audit_item(root=self.root, item=ITEM, config=CONFIG).status,
                         'not_found')
        self.assertFalse(self.paths()[0].exists())

    def test_byte_budget_fails_closed(self):
        result = self.execute(max_bytes=10)
        self.assertEqual((result.state, result.requests), ('failed', 1))
        self.assertEqual(audit_item(root=self.root, item=ITEM, config=CONFIG).status,
                         'not_found')

    def test_network_timeout_then_rerun_then_skip(self):
        def timeout(*args):
            self.calls += 1
            raise urllib.error.URLError('simulated_timeout')
        self.assertEqual(self.execute(transport=timeout).state, 'failed')
        self.assertEqual(audit_item(root=self.root, item=ITEM, config=CONFIG).status,
                         'not_found')
        self.assertEqual(self.execute().state, 'published')
        self.assertEqual(self.execute(transport=timeout).state, 'skipped')
        self.assertEqual(self.calls, 2)

    def test_header_only_valid_empty(self):
        result = self.execute(transport=lambda *_: csv_payload(empty=True))
        self.assertEqual(result.state, 'published')
        self.assertEqual(json.loads(self.paths()[1].read_text())['status'], 'empty')
        self.assertEqual(audit_item(root=self.root, item=ITEM, config=CONFIG).status,
                         'complete')

    def test_bad_date_returns_failed_without_artifact(self):
        result = self.execute(transport=lambda *_: csv_payload(day='2024-11-02'))
        self.assertEqual(result.state, 'failed')
        self.assertFalse(self.paths()[0].exists())

    def test_overlap_replay_manifest_blocks_even_if_live_absent(self):
        path = (self.root / 'data/raw/nasa_firms/manifests/VIIRS_NOAA20_SP/VN_BBOX/'
                'year=2024/month=11/existing.manifest.json')
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(json.dumps({
            'source': 'nasa_firms', 'source_product': 'VIIRS_NOAA20_SP',
            'region_id': 'VN_BBOX', 'start_utc': '2024-11-01T00:00:00Z',
            'end_utc': '2024-11-02T00:00:00Z', 'acquisition_mode': 'replay'
        }), encoding='utf-8')
        self.assertEqual(self.execute().state, 'blocked_review')
        self.assertEqual(self.calls, 0)

    def test_existing_unverified_file_blocks_and_preserves_it(self):
        raw, mf, _ = self.paths()
        raw.parent.mkdir(parents=True, exist_ok=True)
        raw.write_bytes(b'not a valid csv')
        self.assertEqual(self.execute().state, 'blocked_review')
        self.assertEqual(raw.read_bytes(), b'not a valid csv')
        self.assertEqual(self.calls, 0)

    def test_bad_arguments_are_rejected(self):
        with self.assertRaises(ValueError):
            self.execute(max_bytes=True)
        with self.assertRaises(ValueError):
            self.execute(approve_live='yes')
        with self.assertRaises(ValueError):
            self.execute(api_key=None)
        with self.assertRaises(ValueError):
            self.execute(item=PlanItem('firms', 'VN_BBOX', '2024-11-01', '2024-11-07',
                                       product='VIIRS_NOAA20_SP'))
        self.assertEqual(self.calls, 0)

    def test_lock_blocks_without_http(self):
        _, _, key = self.paths()
        locks = self.root / 'data/raw/nasa_firms/_locks'
        locks.mkdir(parents=True, exist_ok=True)
        lock = locks / f'{key}.lock'
        lock.write_text('existing')
        self.assertEqual(self.execute().state, 'blocked_review')
        self.assertEqual(lock.read_text(), 'existing')
        self.assertEqual(self.calls, 0)

    def test_state_change_after_lock_fails_before_http(self):
        with patch('src.ingestion.backfill_firms_live.audit_item') as audit:
            from src.ingestion.backfill_audit import AuditResult
            audit.return_value = AuditResult(ITEM, 'complete', 'race')
            # Preflight gate has its own auditor, which sees not found.
            result = self.execute()
        self.assertEqual(result.state, 'blocked_review')
        self.assertEqual(self.calls, 0)

    def test_main_is_dry_run(self):
        out = io.StringIO()
        with patch('src.ingestion.backfill_firms_live.ROOT', self.root), \
             patch('src.ingestion.backfill_firms_live.validate_config', return_value=CONFIG), \
             redirect_stdout(out):
            code = main(['--region', 'VN_BBOX', '--product', 'VIIRS_NOAA20_SP',
                         '--start', '2024-11-01', '--end', '2024-11-02'])
        self.assertEqual(code, 0)
        self.assertIn('APPROVAL_REQUIRED', out.getvalue())
        self.assertNotIn('PUBLISHED', out.getvalue())
        self.assertEqual(self.calls, 0)
        self.assertEqual(list(self.root.rglob('*')), [])


if __name__ == '__main__':
    unittest.main()
