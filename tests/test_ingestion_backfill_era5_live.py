"""Guarded ERA5: all retrievals and semantic validators here are FAKE / offline."""
from __future__ import annotations

import io
import json
import tempfile
import unittest
from contextlib import redirect_stdout
from datetime import date
from pathlib import Path
from unittest.mock import patch

from src.ingestion.backfill import PlanItem
from src.ingestion.backfill_audit import AuditResult, audit_item
from src.ingestion.backfill_era5_live import (ERA5LiveResult, execute_one_era5_day,
                                              main)
from src.ingestion.era5 import DATASET, _paths, validate_bbox
from src.ingestion.openaq import IngestionError

VARS = ['2m_temperature', '2m_dewpoint_temperature',
        '10m_u_component_of_wind', '10m_v_component_of_wind', 'total_precipitation']
BBOX = dict(north=21.25, west=105.5, south=20.75, east=106)
CONFIG = {'regions': {'HANOI': BBOX},
          'ingestion': {'era5': {'dataset': DATASET, 'variables': VARS}}}
ITEM = PlanItem('era5', 'HANOI', '2024-11-03', '2024-11-04')
PAYLOAD = b'FAKE_GRIB_FOR_ROUTING_TEST_ONLY'


class GuardedERA5Tests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory(prefix='era5-test-')
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name)
        self.calls = 0

    def fake_download(self, ds, request, path):
        self.calls += 1
        self.assertEqual(ds, DATASET)
        self.assertEqual(request['year'], ['2024'])
        self.assertEqual(request['month'], ['11'])
        self.assertEqual(request['day'], ['03'])
        self.assertEqual(len(request['time']), 24)
        self.assertEqual(request['variable'], VARS)
        Path(path).write_bytes(PAYLOAD)

    def fake_validate(self, path, *, day, variables):
        self.assertEqual(day, date(2024, 11, 3))
        self.assertEqual(variables, VARS)
        self.assertEqual(path.read_bytes(), PAYLOAD)
        return 120  # Mock ONLY; live default does real ecCodes semantic validation.

    def execute(self, **extra):
        opts = dict(root=self.root, item=ITEM, config=CONFIG,
                    approve_live=True, max_bytes=4096,
                    downloader=self.fake_download, validator=self.fake_validate)
        opts.update(extra)
        return execute_one_era5_day(**opts)

    def paths(self):
        return _paths(self.root, 'HANOI', date(2024, 11, 3),
                      validate_bbox(BBOX), VARS, 'live')

    def test_publish_identity_checksum_original_bytes_and_cleanup(self):
        result = self.execute()
        self.assertEqual(result.state, 'published')
        self.assertEqual((result.requests, result.messages, self.calls), (1, 120, 1))
        self.assertEqual(result.grib_bytes, len(PAYLOAD))
        raw, mf, _ = self.paths()
        self.assertEqual(raw.read_bytes(), PAYLOAD)
        obj = json.loads(mf.read_text(encoding='utf-8'))
        self.assertEqual((obj['acquisition_mode'], obj['row_count'], obj['status']),
                         ('live', 120, 'success'))
        self.assertEqual(audit_item(root=self.root, item=ITEM, config=CONFIG).status, 'complete')
        self.assertFalse(list(self.root.rglob('*.download')))
        self.assertFalse(list(self.root.rglob('*.lock')))

    def test_skip_after_publish_never_calls_cds(self):
        self.assertEqual(self.execute().state, 'published')
        result = self.execute(downloader=lambda *_: self.fail('CDS should not be called'))
        self.assertEqual((result.state, result.requests), ('skipped', 0))
        self.assertEqual(self.calls, 1)

    def fail(self, message):
        raise AssertionError(message)

    def test_no_approval_does_not_change_disk_or_run(self):
        result = self.execute(approve_live=False)
        self.assertEqual(result.state, 'approval_required')
        self.assertEqual(result.requests, 0)
        self.assertEqual(self.calls, 0)
        self.assertEqual(list(self.root.rglob('*')), [])

    def test_cds_failure_then_rerun_then_skip(self):
        def offline_failure(*args):
            self.calls += 1
            raise IngestionError('cds_retrieval_failed')
        self.assertEqual(self.execute(downloader=offline_failure).state, 'failed')
        self.assertEqual(audit_item(root=self.root, item=ITEM, config=CONFIG).status, 'not_found')
        self.assertEqual(self.execute().state, 'published')
        self.assertEqual(self.execute(downloader=offline_failure).state, 'skipped')
        self.assertEqual(self.calls, 2)

    def test_max_bytes_post_download_fails_closed(self):
        result = self.execute(max_bytes=4)
        self.assertEqual((result.state, result.requests), ('failed', 1))
        self.assertEqual(result.grib_bytes, len(PAYLOAD))
        self.assertEqual(audit_item(root=self.root, item=ITEM, config=CONFIG).status, 'not_found')
        self.assertFalse(self.paths()[0].exists())

    def test_validation_failure_does_not_publish(self):
        def invalid(_path, **kwargs):
            raise IngestionError('grib_missing_requested_fields')
        result = self.execute(validator=invalid)
        self.assertEqual(result.state, 'failed')
        self.assertFalse(self.paths()[0].exists())

    def test_semantically_incomplete_messages_do_not_publish(self):
        result = self.execute(validator=lambda *_args, **_kwargs: 119)
        self.assertEqual(result.state, 'failed')
        self.assertFalse(self.paths()[0].exists())

    def test_alternate_replay_manifest_blocks_live(self):
        mf = self.root / ('data/raw/era5/manifests/' + DATASET +
                          '/HANOI/year=2024/month=11/day=03/replay-other.manifest.json')
        mf.parent.mkdir(parents=True, exist_ok=True)
        mf.write_text(json.dumps({'source': 'era5', 'source_product': DATASET,
                                  'region_id': 'HANOI', 'acquisition_mode': 'replay',
                                  'start_utc': '2024-11-03T00:00:00Z',
                                  'end_utc': '2024-11-04T00:00:00Z'}), encoding='utf-8')
        result = self.execute()
        self.assertEqual(result.state, 'blocked_review')
        self.assertEqual(self.calls, 0)

    def test_existing_raw_without_manifest_blocks_and_preserves(self):
        raw, _, _ = self.paths()
        raw.parent.mkdir(parents=True, exist_ok=True)
        raw.write_bytes(b'precious_artifact')
        self.assertEqual(self.execute().state, 'blocked_review')
        self.assertEqual(raw.read_bytes(), b'precious_artifact')
        self.assertEqual(self.calls, 0)

    def test_stale_or_active_lock_blocks(self):
        _, _, key = self.paths()
        path = self.root / 'data/raw/era5/_locks' / f'{key}.lock'
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text('existing')
        self.assertEqual(self.execute().state, 'blocked_review')
        self.assertEqual(path.read_text(), 'existing')
        self.assertEqual(self.calls, 0)

    def test_identity_change_after_lock_stops_before_download(self):
        with patch('src.ingestion.backfill_era5_live.audit_item',
                   return_value=AuditResult(ITEM, 'complete', 'simulated_race')):
            result = self.execute()
        self.assertEqual(result.state, 'blocked_review')
        self.assertEqual(self.calls, 0)

    def test_identity_change_before_publish_stops_without_publication(self):
        actual_audit = audit_item
        n = [0]
        def change_late(*, root, item, config):
            if root == self.root:
                n[0] += 1
                if n[0] == 2:
                    return AuditResult(item, 'review', 'simulated_race')
            return actual_audit(root=root, item=item, config=config)
        with patch('src.ingestion.backfill_era5_live.audit_item', side_effect=change_late):
            result = self.execute()
        self.assertEqual(result.state, 'blocked_review')
        self.assertEqual(self.calls, 1)
        self.assertFalse(self.paths()[0].exists())

    def test_invalid_requests_and_budgets(self):
        for overrides in ({'max_bytes': True}, {'max_bytes': 0},
                          {'max_bytes': 128*1024*1024+1}, {'approve_live': 'yes'},
                          {'item': PlanItem('era5', 'HANOI', '2024-11-03', '2024-11-05')},
                          {'item': PlanItem('era5', 'HANOI', '2024-11-03', '2024-11-04',
                                            product='bad')},
                          {'item': PlanItem('firms', 'HANOI', '2024-11-03', '2024-11-04',
                                            product='VIIRS_NOAA20_SP')}):
            with self.subTest(overrides=overrides), self.assertRaises(ValueError):
                self.execute(**overrides)
        self.assertEqual(self.calls, 0)

    def test_configuration_requires_valid_variables_dataset(self):
        changed = {'regions': {'HANOI': BBOX}, 'ingestion': {'era5':
                   {'dataset': 'different-era5', 'variables': VARS}}}
        with self.assertRaises(ValueError):
            self.execute(config=changed)
        self.assertEqual(self.calls, 0)

    def test_cli_default_is_dry_run(self):
        out = io.StringIO()
        with patch('src.ingestion.backfill_era5_live.ROOT', self.root), \
             patch('src.ingestion.backfill_era5_live.validate_config', return_value=CONFIG), \
             redirect_stdout(out):
            rc = main(['--region', 'HANOI', '--start', '2024-11-03', '--end', '2024-11-04'])
        self.assertEqual(rc, 0)
        self.assertIn('APPROVAL_REQUIRED', out.getvalue())
        self.assertEqual(self.calls, 0)
        self.assertEqual(list(self.root.rglob('*')), [])


if __name__ == '__main__':
    unittest.main()
