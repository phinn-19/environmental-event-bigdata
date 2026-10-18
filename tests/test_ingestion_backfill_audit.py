"""Offline, side-effect-free tests for strict manifest-aware backfill audits."""
import hashlib
import json
import tempfile
import unittest
from datetime import date
from pathlib import Path

from src.ingestion.backfill import PlanItem
from src.ingestion.backfill_audit import audit_item
from src.ingestion.era5 import _paths as era5_paths, validate_bbox
from src.ingestion.firms import _paths as firms_paths, valid_bbox
from src.ingestion.manifest import request_id
from src.ingestion.openaq import _paths as openaq_paths


CONFIG = {
    'regions': {
        'HANOI': {'north': 21.25, 'west': 105.5, 'south': 20.75, 'east': 106.0},
        'VN_BBOX': {'north': 24.0, 'west': 102.0, 'south': 8.0, 'east': 110.0},
    },
    'ingestion': {
        'openaq': {'page_limit': 100},
        'era5': {'variables': [
            '2m_temperature', '2m_dewpoint_temperature', '10m_u_component_of_wind',
            '10m_v_component_of_wind', 'total_precipitation',
        ]},
    },
}


def put(root, raw, mf, payload, *, source, product, region, start, end, req_id,
        mode='live', rows=24, status='success'):
    raw.parent.mkdir(parents=True, exist_ok=True)
    mf.parent.mkdir(parents=True, exist_ok=True)
    raw.write_bytes(payload)
    manifest = {
        'schema_version': 'manifest-v1', 'source': source,
        'source_product': product, 'acquisition_mode': mode,
        'region_id': region, 'start_utc': start + 'T00:00:00Z',
        'end_utc': end + 'T00:00:00Z', 'request_id': req_id,
        'status': status, 'format': {'openaq': 'json', 'nasa_firms': 'csv', 'era5': 'grib'}[source],
        'row_count': rows, 'bytes': len(payload),
        'file': raw.relative_to(root).as_posix(),
        'sha256': hashlib.sha256(payload).hexdigest(),
    }
    mf.write_text(json.dumps(manifest), encoding='utf-8')
    return manifest


class BackfillAuditTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.root = Path(self.tmp.name)

    def check(self, item):
        return audit_item(root=self.root, item=item, config=CONFIG)

    def test_openaq_not_found(self):
        item = PlanItem('openaq', 'HANOI', '2024-11-01', '2024-11-02', sensor_id=21632)
        self.assertEqual(self.check(item).status, 'not_found')

    def add_aq(self, *, mode='live', rows=24, page=1, found=None):
        raw, mf = openaq_paths(self.root, 21632, date(2024, 11, 1), page)
        payload = json.dumps({'results': [{}] * rows, 'meta': {'found': found}}).encode()
        m = put(self.root, raw, mf, payload, source='openaq', product='openaq_v3_hours',
                region='HANOI', start='2024-11-01', end='2024-11-02',
                req_id=request_id('openaq', 21632, '2024-11-01', page),
                mode=mode, rows=rows, status='empty' if not rows else 'success')
        return raw, mf, m

    def test_openaq_sha_complete(self):
        self.add_aq()
        self.assertEqual(self.check(PlanItem('openaq', 'HANOI', '2024-11-01', '2024-11-02', sensor_id=21632)).status, 'complete')

    def test_openaq_replay_is_review_not_live_complete(self):
        self.add_aq(mode='replay')
        out = self.check(PlanItem('openaq', 'HANOI', '2024-11-01', '2024-11-02', sensor_id=21632))
        self.assertEqual(out.status, 'review')

    def test_openaq_checksum_tamper_review(self):
        raw, _, _ = self.add_aq()
        raw.write_bytes(raw.read_bytes() + b'changed')
        self.assertEqual(self.check(PlanItem('openaq', 'HANOI', '2024-11-01', '2024-11-02', sensor_id=21632)).status, 'review')

    def test_openaq_full_page_unproven_review(self):
        self.add_aq(rows=100)
        out = self.check(PlanItem('openaq', 'HANOI', '2024-11-01', '2024-11-02', sensor_id=21632))
        self.assertEqual(out.status, 'review')
        self.assertEqual(out.reason, 'pagination_terminal_unproven')

    def test_openaq_complete_when_found_count_proves_terminal(self):
        self.add_aq(rows=100, found=100)
        self.assertEqual(self.check(PlanItem('openaq', 'HANOI', '2024-11-01', '2024-11-02', sensor_id=21632)).status, 'complete')

    def test_openaq_short_terminal_but_exact_found_count_higher_needs_review(self):
        self.add_aq(rows=1, found=24)
        result = self.check(PlanItem('openaq', 'HANOI', '2024-11-01', '2024-11-02', sensor_id=21632))
        self.assertEqual(result.status, 'review')
        self.assertEqual(result.reason, 'pagination_found_count_mismatch')

    def test_openaq_multi_page_exact_found_count_agrees(self):
        self.add_aq(rows=100, page=1, found=120)
        self.add_aq(rows=20, page=2, found=120)
        result = self.check(PlanItem('openaq', 'HANOI', '2024-11-01', '2024-11-02', sensor_id=21632))
        self.assertEqual(result.status, 'complete')

    def test_openaq_missing_middle_page_review(self):
        self.add_aq(rows=100, page=1)
        self.add_aq(rows=12, page=3)
        out = self.check(PlanItem('openaq', 'HANOI', '2024-11-01', '2024-11-02', sensor_id=21632))
        self.assertEqual(out.status, 'review')

    def test_firms_complete_empty_is_valid(self):
        day = date(2024, 11, 1)
        raw, mf, key = firms_paths(self.root, 'VIIRS_NOAA20_SP', 'VN_BBOX', day, 1,
                                   valid_bbox(CONFIG['regions']['VN_BBOX']), 'live')
        put(self.root, raw, mf, b'header\n', source='nasa_firms', product='VIIRS_NOAA20_SP',
            region='VN_BBOX', start='2024-11-01', end='2024-11-02',
            req_id=key, rows=0, status='empty')
        item = PlanItem('firms', 'VN_BBOX', '2024-11-01', '2024-11-02', product='VIIRS_NOAA20_SP')
        self.assertEqual(self.check(item).status, 'complete')

    def test_firms_different_window_overlap_needs_review(self):
        day = date(2024, 11, 1)
        raw, mf, key = firms_paths(self.root, 'VIIRS_NOAA20_SP', 'VN_BBOX', day, 1,
                                   valid_bbox(CONFIG['regions']['VN_BBOX']), 'live')
        put(self.root, raw, mf, b'header\n', source='nasa_firms', product='VIIRS_NOAA20_SP',
            region='VN_BBOX', start='2024-11-01', end='2024-11-02',
            req_id=key, rows=0, status='empty')
        bigger = PlanItem('firms', 'VN_BBOX', '2024-11-01', '2024-11-06', product='VIIRS_NOAA20_SP')
        self.assertEqual(self.check(bigger).status, 'review')

    def test_era5_complete_if_matching_request_and_120_messages(self):
        day = date(2024, 11, 1)
        raw, mf, key = era5_paths(self.root, 'HANOI', day,
                                  validate_bbox(CONFIG['regions']['HANOI']),
                                  CONFIG['ingestion']['era5']['variables'], 'live')
        put(self.root, raw, mf, b'fake grib bytes for manifest audit only',
            source='era5', product='reanalysis-era5-single-levels',
            region='HANOI', start='2024-11-01', end='2024-11-02', req_id=key, rows=120)
        self.assertEqual(self.check(PlanItem('era5', 'HANOI', '2024-11-01', '2024-11-02')).status, 'complete')

    def test_era5_wrong_count_needs_review(self):
        day = date(2024, 11, 1)
        raw, mf, key = era5_paths(self.root, 'HANOI', day,
                                  validate_bbox(CONFIG['regions']['HANOI']),
                                  CONFIG['ingestion']['era5']['variables'], 'live')
        put(self.root, raw, mf, b'metadata dummy', source='era5',
            product='reanalysis-era5-single-levels', region='HANOI',
            start='2024-11-01', end='2024-11-02', req_id=key, rows=24)
        self.assertEqual(self.check(PlanItem('era5', 'HANOI', '2024-11-01', '2024-11-02')).status, 'review')

    def test_no_files_written_for_missing(self):
        item = PlanItem('era5', 'HANOI', '2024-11-01', '2024-11-02')
        self.check(item)
        self.assertEqual(list(self.root.rglob('*')), [])


if __name__ == '__main__':
    unittest.main()
