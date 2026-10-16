"""Offline contract tests for the first real ingestion module.

No network, no API key, no Spark. Temporary directories only.
"""
import io
import json
import tempfile
import unittest
import urllib.error
from pathlib import Path
from urllib.parse import parse_qs, urlparse

from src.ingestion.manifest import sha256_bytes
from src.ingestion.openaq import IngestionError, ingest_openaq, parse_days, parse_payload


def page_bytes(rows, *, found=None):
    return json.dumps({'meta': {'page': 1, 'found': found}, 'results': rows},
                      ensure_ascii=False).encode('utf-8')


def measure(hour, value, flagged=False):
    return {
        'value': value,
        'flagInfo': {'hasFlags': flagged},
        'parameter': {'id': 2, 'name': 'pm25', 'units': 'µg/m³'},
        'period': {'datetimeFrom': {'utc': f'2024-11-01T{hour:02d}:00:00Z'},
                   'datetimeTo': {'utc': f'2024-11-01T{hour+1:02d}:00:00Z'}},
    }


class OpenAQIngestionTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.root = Path(self.tmp.name)

    def run_ingest(self, **kwargs):
        options = dict(root=self.root, region_id='HANOI', sensor_ids=[21632],
                       start='2024-11-01', end='2024-11-02', api_key='TEST_KEY',
                       page_limit=100, sleep=lambda _: None)
        options.update(kwargs)
        return ingest_openaq(**options)

    def manifests(self):
        return sorted(self.root.glob('data/raw/openaq/manifests/**/*.manifest.json'))

    def test_parse_end_exclusive_days(self):
        days = parse_days('2024-11-01', '2024-11-03')
        self.assertEqual([d.isoformat() for d in days], ['2024-11-01','2024-11-02'])
        for start, end in [('2024-11-01','2024-11-01'), ('2024-11-03','2024-11-01')]:
            with self.assertRaises(ValueError):
                parse_days(start, end)

    def test_live_save_verbatim_keep_null_and_flag(self):
        body = page_bytes([measure(0, None, True), measure(1, 127.0, True)])
        received = []
        def transport(url, headers, timeout):
            received.append((url, headers))
            return body
        summary = self.run_ingest(transport=transport)
        self.assertTrue(summary.ok)
        self.assertEqual(summary.rows, 2)
        self.assertEqual(len(received), 1)
        self.assertIn('datetime_from=2024-11-01T00%3A00%3A00Z', received[0][0])
        self.assertEqual(received[0][1]['X-API-Key'], 'TEST_KEY')
        manifest = json.loads(self.manifests()[0].read_text(encoding='utf8'))
        self.assertEqual(manifest['row_count'], 2)
        self.assertEqual(manifest['sha256'], sha256_bytes(body))
        self.assertEqual(manifest['acquisition_mode'], 'live')
        self.assertNotIn('TEST_KEY', str(manifest))
        self.assertEqual((self.root / manifest['file']).read_bytes(), body)
        stored_rows = json.loads((self.root / manifest['file']).read_text(encoding='utf8'))['results']
        self.assertIsNone(stored_rows[0]['value'])
        self.assertTrue(stored_rows[0]['flagInfo']['hasFlags'])

    def test_pagination_not_dependent_on_meta_found_integer(self):
        bodies = [page_bytes([measure(0, 1), measure(1, 2)], found='>2'),
                  page_bytes([measure(2, 3)], found='>2')]
        def transport(url, headers, timeout):
            page = int(parse_qs(urlparse(url).query)['page'][0])
            return bodies[page - 1]
        summary = self.run_ingest(page_limit=2, transport=transport)
        self.assertTrue(summary.ok)
        self.assertEqual((summary.pages, summary.rows), (2, 3))
        self.assertEqual(len(self.manifests()), 2)

    def test_empty_page_has_zero_count(self):
        summary = self.run_ingest(transport=lambda *_: page_bytes([]))
        self.assertTrue(summary.ok)
        manifest = json.loads(self.manifests()[0].read_text(encoding='utf8'))
        self.assertEqual(manifest['status'], 'empty')
        self.assertEqual(manifest['row_count'], 0)
        self.assertEqual(summary.empty_pages, 1)

    def test_http_429_retries_without_exposing_key(self):
        attempts = []
        def transport(url, headers, timeout):
            attempts.append(True)
            if len(attempts) == 1:
                raise urllib.error.HTTPError(url, 429, 'rate limited', {}, io.BytesIO())
            return page_bytes([measure(1, 20)])
        summary = self.run_ingest(transport=transport, max_retries=1)
        self.assertTrue(summary.ok)
        self.assertEqual(len(attempts), 2)

    def test_http_401_failure_manifest_no_raw_file(self):
        def transport(url, headers, timeout):
            raise urllib.error.HTTPError(url, 401, 'unauthorized', {}, io.BytesIO())
        summary = self.run_ingest(transport=transport)
        self.assertFalse(summary.ok)
        manifest = json.loads(self.manifests()[0].read_text(encoding='utf8'))
        self.assertEqual(manifest['status'], 'failed')
        self.assertEqual(manifest['error_type'], 'http_401')
        self.assertIsNone(manifest['file'])
        self.assertIsNone(manifest['sha256'])
        self.assertNotIn('TEST_KEY', json.dumps(manifest))

    def test_bad_response_failure_manifest(self):
        summary = self.run_ingest(transport=lambda *_: b'<html>Not JSON</html>')
        self.assertFalse(summary.ok)
        manifest = json.loads(self.manifests()[0].read_text(encoding='utf8'))
        self.assertEqual(manifest['error_type'], 'invalid_json')

    def test_no_silent_out_of_window(self):
        body = page_bytes([{'period': {'datetimeFrom': {'utc': '2024-11-02T00:00:00Z'}}}])
        with self.assertRaises(IngestionError) as ctx:
            parse_payload(body, parse_days('2024-11-01', '2024-11-02')[0])
        self.assertEqual(ctx.exception.code, 'out_of_window_measurement')

    def test_replay_mode_uses_snapshot_without_network(self):
        sample = self.root / 'sample.json'
        sample.write_bytes(page_bytes([measure(1, 127, True)]))
        def fail_if_network(*_):
            raise AssertionError('replay must never call HTTP')
        summary = self.run_ingest(replay_file=sample, api_key=None, transport=fail_if_network)
        self.assertTrue(summary.ok)
        manifest = json.loads(self.manifests()[0].read_text(encoding='utf8'))
        self.assertEqual(manifest['acquisition_mode'], 'replay')

    def test_pagination_cap_signals_incomplete_result(self):
        body = page_bytes([measure(1, 20)], found='>1')
        summary = self.run_ingest(page_limit=1, max_pages=1, transport=lambda *_: body)
        self.assertFalse(summary.ok)
        self.assertEqual(summary.rows, 1)
        failed = [json.loads(p.read_text(encoding='utf8')) for p in self.manifests() if 'page=0002.' in p.name]
        self.assertEqual(failed[0]['error_type'], 'pagination_limit_reached')

    def test_different_bytes_cannot_overwrite_without_flag(self):
        self.assertTrue(self.run_ingest(transport=lambda *_: page_bytes([measure(1, 20)])).ok)
        summary = self.run_ingest(transport=lambda *_: page_bytes([measure(1, 30)]))
        self.assertFalse(summary.ok)
        self.assertEqual(summary.failures, 1)
        first = list(self.root.glob('data/raw/openaq/hours/**/*.json'))[0]
        self.assertEqual(json.loads(first.read_text())['results'][0]['value'], 20)


if __name__ == '__main__':
    unittest.main()
