"""Offline fake-bucket tests; NO user credentials or cloud I/O."""
from __future__ import annotations

import hashlib
import json
import sys
import tempfile
from pathlib import Path
from types import SimpleNamespace
import unittest

SCRIPT_DIR = Path(__file__).resolve().parents[1] / 'scripts' / 'bulk'
sys.path.insert(0, str(SCRIPT_DIR))
# Source base scripts are already present in main after PR #31/#32.
# In the isolated patch test, use their exact local original sources.
FALLBACK = Path(__file__).resolve().parents[2] / 'ERA5_LAKE_VERIFY_READONLY_20261103' / 'scripts' / 'bulk'
if FALLBACK.is_dir():
    sys.path.insert(1, str(FALLBACK))
FALLBACK_PREFLIGHT = Path(__file__).resolve().parents[2] / 'ERA5_CANONICAL_PREFLIGHT_20261103' / 'scripts' / 'bulk'
if FALLBACK_PREFLIGHT.is_dir():
    sys.path.insert(1, str(FALLBACK_PREFLIGHT))

import era5_hf_staging_verify as upstream
import era5_canonical_promote_guarded as mod

SCHEMA = {
    '$schema': 'https://json-schema.org/draft/2020-12/schema',
    'type': 'object', 'additionalProperties': False,
    'required': ['source', 'region_id', 'start_utc', 'end_utc',
                 'retrieved_at_utc', 'format', 'file', 'status', 'row_count', 'sha256'],
    'properties': {
        'schema_version': {'type': 'string', 'const': 'manifest-v1'},
        'source': {'enum': ['era5', 'openaq', 'nasa_firms']},
        'source_product': {'type': 'string'},
        'acquisition_mode': {'enum': ['live', 'replay']},
        'region_id': {'type': 'string'},
        'start_utc': {'type': ['string', 'null'], 'format': 'date-time'},
        'end_utc': {'type': ['string', 'null'], 'format': 'date-time'},
        'retrieved_at_utc': {'type': 'string', 'format': 'date-time'},
        'format': {'enum': ['grib', 'json', 'csv']},
        'file': {'type': ['string', 'null']},
        'status': {'enum': ['success', 'empty', 'partial', 'failed']},
        'row_count': {'type': ['integer', 'null'], 'minimum': 0},
        'sha256': {'type': ['string', 'null']},
        'bytes': {'type': ['integer', 'null'], 'minimum': 0},
        'request_id': {'type': 'string', 'minLength': 1},
        'error_type': {'type': ['string', 'null']},
    }
}


def fake_state_and_manifest(m, body):
    sha = hashlib.sha256(body).hexdigest()
    rid = 'aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee'
    common = {
        'month': m, 'dataset': upstream.DATASET,
        'expected_fields': upstream.expected_fields(m), 'audit_status': 'PASS',
        'size_bytes': len(body), 'sha256': sha, 'request_id': rid,
        'request_sha256': upstream.expected_request_sha(m),
    }
    state = {**common, 'status': 'PASS', 'request': upstream.expected_request(m),
             'audited_grib_messages': common['expected_fields'],
             'unique_date_hour_parameter': common['expected_fields'],
             'grid_dimensions': '33x65', 'audit_time_utc': '2026-11-03T00:00:00+00:00'}
    manifest = {**common, 'bbox_nwse': [24, 102, 8, 110], 'data_format': 'grib',
                'cloud_prefix': upstream.PREFIX, 'variable': list(upstream.VARIABLES),
                'audit_method': 'ecCodes date/hour/parameter/grid + SHA-256'}
    return state, manifest


class FakeBucket:
    def __init__(self, months=('2023-08',)):
        self.objects = {}
        self.upload_calls = []
        self.private = True
        for m in months:
            raw = (f'FAKE-GRIB-{m}').encode()
            st, mf = fake_state_and_manifest(m, raw)
            self.objects[upstream.key_for(m, 'raw')] = raw
            self.objects[upstream.key_for(m, 'state')] = json.dumps(st).encode()
            self.objects[upstream.key_for(m, 'manifest')] = json.dumps(mf).encode()

    def info(self, bucket, token=''):
        return SimpleNamespace(private=self.private, size=sum(map(len, self.objects.values())))

    def tree(self, bucket, prefix='', recursive=True, token=''):
        return [SimpleNamespace(path=k, size=len(v), type='file')
                for k, v in self.objects.items() if k.startswith(prefix)]

    def download(self, bucket, files, token='', raise_on_missing_files=True):
        for key, path in files:
            if key not in self.objects:
                raise FileNotFoundError('mock missing key')
            Path(path).write_bytes(self.objects[key])

    def upload(self, bucket, add, token=''):
        for local, key in add:
            if key in self.objects:
                raise RuntimeError('test prohibits overwrite')
            self.objects[key] = Path(local).read_bytes()
            self.upload_calls.append(key)

    def run(self, mode='check', months=('2023-08',), read_token='read', write_token='write'):
        return mod.run_remote(mode=mode, months=months, bucket=mod.BUCKET,
                              read_token=read_token, write_token=write_token,
                              schema=SCHEMA, get_info=self.info, list_tree=self.tree,
                              download=self.download, upload=self.upload if mode == 'promote' else None)


class PromoteTests(unittest.TestCase):
    def test_month_paths_and_guard(self):
        raw, mf = mod.canonical_paths('2023-08')
        self.assertTrue(raw.startswith('data/raw/era5/reanalysis-era5-single-levels/VN_BBOX/year=2023/month=08/'))
        self.assertTrue(mf.startswith('data/raw/era5/manifests/'))
        with self.assertRaises(ValueError):
            mod.canonical_paths('2024-01')
        with self.assertRaises(ValueError):
            mod.parse_months('2023-08,2023-08')

    def test_single_historical_month_is_allowed_but_other_bulk_is_refused(self):
        self.assertEqual(mod.parse_months('2023-07'), ('2023-07',))
        self.assertEqual(mod.parse_months('2021-01'), ('2021-01',))
        self.assertEqual(mod.parse_months(','.join(upstream.MONTHS)), upstream.MONTHS)
        with self.assertRaises(ValueError):
            mod.parse_months('2023-06,2023-07')
        with self.assertRaises(ValueError):
            mod.parse_months('2024-02')  # already audited locally
        with self.assertRaises(ValueError):
            mod.parse_months('2023-12')  # historical queue deliberately excludes it
        with self.assertRaises(ValueError):
            mod.parse_months('2020-12')
        with self.assertRaises(ValueError):
            mod.parse_months('2023-07,')

    def test_july_2023_single_month_check_promote_and_idempotence(self):
        b = FakeBucket(months=('2023-07',))
        checked = b.run(mode='check', months=('2023-07',))
        self.assertEqual(checked['source_bytes_verified'], len(b'FAKE-GRIB-2023-07'))
        self.assertEqual(checked['actions'][0]['status'], 'NEW_RAW_AND_MANIFEST')
        self.assertEqual(b.upload_calls, [])
        promoted = b.run(mode='promote', months=('2023-07',))
        self.assertEqual(promoted['hf_writes'], 2)
        self.assertEqual(b.run(mode='check', months=('2023-07',))['actions'][0]['status'],
                         'ALREADY_PRESENT')
        self.assertEqual(b.run(mode='promote', months=('2023-07',))['hf_writes'], 0)
        self.assertEqual(len(b.upload_calls), 2)

    def test_july_missing_state_fails_closed(self):
        b = FakeBucket(months=('2023-07',))
        del b.objects[upstream.key_for('2023-07', 'state')]
        with self.assertRaisesRegex(ValueError, 'missing staged'):
            b.run(mode='check', months=('2023-07',))
        self.assertFalse(b.upload_calls)

    def test_manifest_v1_and_month_bounds(self):
        state, source = fake_state_and_manifest('2023-08', b'dummy')
        with tempfile.TemporaryDirectory() as t:
            p = Path(t) / 'raw.grib'
            p.write_bytes(b'dummy')
            out = mod.build_manifest('2023-08', state, source, p, SCHEMA)
            self.assertEqual(out['row_count'], 3720)
            self.assertEqual(out['end_utc'], '2023-09-01T00:00:00Z')
            self.assertEqual(out['retrieved_at_utc'], '2026-11-03T00:00:00Z')
            self.assertEqual(out['request_id'], state['request_id'])

    def test_schema_rejects_unreviewed_extras(self):
        st, source = fake_state_and_manifest('2023-08', b'dummy')
        with tempfile.TemporaryDirectory() as t:
            p = Path(t) / 'raw.grib'
            p.write_bytes(b'dummy')
            doc = mod.build_manifest('2023-08', st, source, p, SCHEMA)
            doc['unknown'] = 'not allowed'
            with self.assertRaises(Exception):
                mod.validate_manifest(doc, SCHEMA)

    def test_read_only_check_does_not_write(self):
        b = FakeBucket()
        result = b.run(mode='check')
        self.assertEqual(result['hf_writes'], 0)
        self.assertEqual(result['actions'][0]['status'], 'NEW_RAW_AND_MANIFEST')
        self.assertEqual(b.upload_calls, [])

    def test_real_promotion_mock_upload_order_and_rerun(self):
        b = FakeBucket()
        result = b.run(mode='promote')
        self.assertEqual(result['hf_writes'], 2)
        self.assertEqual(len(b.upload_calls), 2)
        self.assertTrue(b.upload_calls[0].endswith('.grib'))
        self.assertTrue(b.upload_calls[1].endswith('.manifest.json'))
        previous = list(b.upload_calls)
        result2 = b.run(mode='promote')
        self.assertEqual(result2['hf_writes'], 0)
        self.assertEqual(previous, b.upload_calls)

    def test_partial_failure_safe_resume(self):
        b = FakeBucket()
        # Simulate crash after Raw was uploaded, no canonical manifest.
        b.run(mode='promote')
        _, mf = mod.canonical_paths('2023-08')
        b.objects.pop('lake/v1/' + mf)
        result = b.run(mode='promote')
        self.assertEqual(result['hf_writes'], 1)
        self.assertEqual(result['actions'][0]['status'], 'RESUME_MANIFEST')

    def test_refuse_other_overlap(self):
        b = FakeBucket()
        b.run(mode='promote')
        raw, mf = mod.canonical_paths('2023-08')
        doc = json.loads(b.objects['lake/v1/' + mf])
        doc['file'] = raw
        other = ('lake/v1/data/raw/era5/manifests/'
                 'reanalysis-era5-single-levels/VN_BBOX/year=2023/month=08/other.manifest.json')
        b.objects[other] = json.dumps(doc).encode()
        with self.assertRaisesRegex(ValueError, 'overlap'):
            b.run(mode='check')

    def test_refuse_existing_raw_wrong_sha(self):
        b = FakeBucket()
        raw, _ = mod.canonical_paths('2023-08')
        b.objects['lake/v1/' + raw] = b'incorrect bytes'
        with self.assertRaisesRegex(ValueError, 'differs'):
            b.run(mode='check')

    def test_existing_hanoi_manifest_is_allowed_but_target_remains_vn_bbox(self):
        """Regression: existing HANOI manifest must not block VN_BBOX pilot."""
        b = FakeBucket()
        b.run(mode='promote')
        raw, mf = mod.canonical_paths('2023-08')
        old_raw, old_mf = 'lake/v1/' + raw, 'lake/v1/' + mf
        doc = json.loads(b.objects.pop(old_mf))
        raw_data = b.objects.pop(old_raw)

        # Same dates but different region: distinct region scope, as in the
        # canonical preflight. Existing canonical is still fully schema-valid.
        doc['region_id'] = 'HANOI'
        doc['file'] = ('data/raw/era5/reanalysis-era5-single-levels/'
                       'HANOI/year=2023/month=08/day=01/existing.grib')
        hanoi_manifest = ('lake/v1/data/raw/era5/manifests/'
                          'reanalysis-era5-single-levels/HANOI/year=2023/'
                          'month=08/day=01/existing.manifest.json')
        b.objects['lake/v1/' + doc['file']] = raw_data
        b.objects[hanoi_manifest] = json.dumps(doc).encode()

        previous = len(b.upload_calls)
        out = b.run(mode='check')
        self.assertEqual(len(b.upload_calls), previous)
        self.assertEqual(out['canonical_checked_manifests'], 1)
        self.assertEqual(out['actions'][0]['status'], 'NEW_RAW_AND_MANIFEST')
        with self.assertRaisesRegex(ValueError, 'unexpected target region'):
            mod.validate_manifest(doc, SCHEMA)

    def test_private_guard_and_no_write_token(self):
        b = FakeBucket()
        b.private = False
        with self.assertRaisesRegex(ValueError, 'PRIVATE'):
            b.run(mode='check')
        b.private = True
        with self.assertRaisesRegex(ValueError, 'write token'):
            b.run(mode='promote', write_token='')


if __name__ == '__main__':
    unittest.main()
