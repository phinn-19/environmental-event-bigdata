"""Offline NASA FIRMS Area API ingestion tests; no MAP_KEY and no network required."""
import io
import json
import tempfile
import unittest
import urllib.error
from datetime import date
from pathlib import Path

from src.ingestion.firms import ingest_firms, inspect_csv, windows
from src.ingestion.manifest import sha256_bytes

BBOX = {'west': 107.0, 'south': 11.0, 'east': 109.0, 'north': 15.0}
HEADER = 'latitude,longitude,bright_ti4,scan,track,acq_date,acq_time,satellite,instrument,confidence,version,bright_ti5,frp,daynight\n'
RECORD = '11.17867,107.44868,336.78,0.39,0.36,2026-10-09,616,N20,VIIRS,n,2.0NRT,292.69,3.58,D\n'
PRODUCT = 'VIIRS_NOAA20_NRT'


class FIRMSIngestionTests(unittest.TestCase):
    def setUp(self):
        d = tempfile.TemporaryDirectory()
        self.addCleanup(d.cleanup)
        self.root = Path(d.name)

    def ingest(self, **extra):
        kwargs = dict(root=self.root, region_id='VN_BBOX', bbox=BBOX, product=PRODUCT,
                      start='2026-10-09', end='2026-10-10', api_key='SECRET_KEY_TEST_ONLY',
                      transport=lambda *_: (HEADER + RECORD).encode('utf-8'), sleep=lambda _: None)
        kwargs.update(extra)
        return ingest_firms(**kwargs)

    def manifests(self):
        return [json.loads(p.read_text(encoding='utf-8')) for p in
                self.root.glob('data/raw/nasa_firms/manifests/**/*.manifest.json')]

    def test_live_payload_bytes_manifest_and_correct_bbox_url(self):
        expected_bytes = (HEADER + RECORD).encode('utf-8')
        received = []
        def transport(url, timeout):
            received.append((url, timeout))
            return expected_bytes
        summary = self.ingest(transport=transport)
        self.assertTrue(summary.ok)
        self.assertEqual((summary.chunks, summary.rows), (1, 1))
        self.assertIn('/VIIRS_NOAA20_NRT/107,11,109,15/1/2026-10-09', received[0][0])
        self.assertIn('SECRET_KEY_TEST_ONLY', received[0][0])
        manifest = self.manifests()[0]
        self.assertEqual(manifest['source'], 'nasa_firms')
        self.assertEqual(manifest['source_product'], PRODUCT)
        self.assertEqual(manifest['format'], 'csv')
        self.assertEqual(manifest['acquisition_mode'], 'live')
        self.assertEqual(manifest['row_count'], 1)
        self.assertEqual(manifest['sha256'], sha256_bytes(expected_bytes))
        self.assertEqual(manifest['start_utc'], '2026-10-09T00:00:00Z')
        self.assertEqual(manifest['end_utc'], '2026-10-10T00:00:00Z')
        self.assertNotIn('SECRET_KEY_TEST_ONLY', str(manifest))
        self.assertEqual((self.root / manifest['file']).read_bytes(), expected_bytes)

    def test_replay_preserves_sample_no_network(self):
        replay = self.root / 'sample.csv'
        # Small representative CSV, without depending on external samples.
        replay.write_bytes((HEADER + RECORD).encode())
        def no_network(*_):
            raise AssertionError('network requested during replay')
        summary = self.ingest(start='2026-10-09', end='2026-10-14',
                              bbox={'west': 102, 'south': 8, 'east': 110, 'north': 24},
                              replay_file=replay, api_key=None, transport=no_network)
        self.assertTrue(summary.ok)
        self.assertEqual(summary.rows, 1)
        m = self.manifests()[0]
        self.assertEqual(m['acquisition_mode'], 'replay')
        self.assertEqual((self.root / m['file']).read_bytes(), replay.read_bytes())

    def test_empty_header_is_valid_not_failed(self):
        summary = self.ingest(transport=lambda *_: HEADER.encode())
        self.assertTrue(summary.ok)
        self.assertEqual(summary.empty_chunks, 1)
        m = self.manifests()[0]
        self.assertEqual(m['status'], 'empty')
        self.assertEqual(m['row_count'], 0)
        self.assertIsNotNone(m['file'])

    def test_non_csv_body_rejected(self):
        summary = self.ingest(transport=lambda *_: b'<html>Error token hidden</html>')
        self.assertFalse(summary.ok)
        m = self.manifests()[0]
        self.assertEqual(m['error_type'], 'invalid_firms_csv_header')
        self.assertIsNone(m['file'])
        self.assertIsNone(m['sha256'])

    def test_http_401_never_exposes_key(self):
        def transport(url, timeout):
            raise urllib.error.HTTPError(url, 401, 'unauthorized', {}, io.BytesIO())
        self.assertFalse(self.ingest(transport=transport).ok)
        m = self.manifests()[0]
        self.assertEqual(m['error_type'], 'http_401')
        self.assertNotIn('SECRET_KEY_TEST_ONLY', str(m))

    def test_http_429_retries(self):
        calls = []
        def transport(url, timeout):
            calls.append(1)
            if len(calls) == 1:
                raise urllib.error.HTTPError(url, 429, 'rate limited', {}, io.BytesIO())
            return (HEADER + RECORD).encode()
        self.assertTrue(self.ingest(transport=transport, max_retries=2).ok)
        self.assertEqual(len(calls), 2)

    def test_two_chunks_end_exclusive_and_inclusive_nasa_date(self):
        requests = []
        def transport(url, timeout):
            requests.append(url)
            return HEADER.encode()
        s = self.ingest(start='2026-10-09', end='2026-10-15', transport=transport)
        self.assertTrue(s.ok)
        self.assertEqual((s.chunks, s.empty_chunks), (2, 2))
        self.assertIn('/5/2026-10-09', requests[0])
        self.assertIn('/1/2026-10-14', requests[1])
        self.assertEqual(sorted(m['end_utc'] for m in self.manifests()),
                         ['2026-10-14T00:00:00Z', '2026-10-15T00:00:00Z'])

    def test_out_of_window_rejected_without_raw(self):
        bad = HEADER + RECORD.replace('2026-10-09', '2026-10-10')
        self.assertFalse(self.ingest(transport=lambda *_: bad.encode()).ok)
        self.assertEqual(self.manifests()[0]['error_type'], 'out_of_window_hotspot')
        self.assertIsNone(self.manifests()[0]['file'])

    def test_outside_bbox_rejected(self):
        bad = HEADER + RECORD.replace('107.44868', '115.5')
        self.assertFalse(self.ingest(transport=lambda *_: bad.encode()).ok)
        self.assertEqual(self.manifests()[0]['error_type'], 'out_of_region_hotspot')

    def test_sp_vs_nrt_cannot_mix_silently(self):
        self.assertFalse(self.ingest(product='VIIRS_NOAA20_SP').ok)
        self.assertEqual(self.manifests()[0]['error_type'], 'source_product_mismatch')

    def test_bad_hhmm_invalid(self):
        bad = HEADER + RECORD.replace(',616,', ',2460,')
        self.assertFalse(self.ingest(transport=lambda *_: bad.encode()).ok)
        self.assertEqual(self.manifests()[0]['error_type'], 'invalid_firms_coordinates_or_time')

    def test_no_overwrite_different_bytes(self):
        self.assertTrue(self.ingest().ok)
        original = self.manifests()[0]
        new = HEADER + RECORD.replace('3.58,D', '4.00,D')
        self.assertFalse(self.ingest(transport=lambda *_: new.encode()).ok)
        m = self.manifests()[0]
        self.assertEqual(original, m)
        self.assertEqual((self.root / m['file']).read_bytes(), (HEADER + RECORD).encode())

    def test_empty_date_and_large_window_rejected(self):
        with self.assertRaises(ValueError):
            windows('2026-10-09', '2026-10-09')
        with self.assertRaises(ValueError):
            windows('2026-10-09', '2026-10-15', chunk_days=6)
        with self.assertRaises(ValueError):
            self.ingest(product='VIIRS_NOAA21_NRT')

    def test_bom_csv_and_padded_time_accepted(self):
        data = (HEADER + RECORD).encode('utf-8-sig')
        count = inspect_csv(data, begin=date(2026, 10, 9), days=1,
                            bbox=(107, 11, 109, 15), product=PRODUCT)
        self.assertEqual(count, 1)

    def test_manifest_replay_and_live_separate_paths(self):
        raw = self.root / 'snap.csv'
        raw.write_bytes((HEADER + RECORD).encode())
        self.assertTrue(self.ingest(replay_file=raw, api_key=None).ok)
        self.assertTrue(self.ingest().ok)
        m = self.manifests()
        self.assertEqual(len(m), 2)
        self.assertEqual({r['acquisition_mode'] for r in m}, {'replay', 'live'})
        self.assertEqual(len({r['file'] for r in m}), 2)


if __name__ == '__main__':
    unittest.main()
