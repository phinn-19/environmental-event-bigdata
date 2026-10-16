"""No-network OpenAQ location/sensor metadata tests for Sprint 01B."""
import copy
import io
import json
import tempfile
import unittest
import urllib.error
from pathlib import Path

from src.ingestion.manifest import sha256_bytes
from src.ingestion.openaq import IngestionError
from src.ingestion.openaq_metadata import ingest_openaq_location, inspect_location_payload


STATION = {
    'id': 7441, 'name': 'Hanoi', 'timezone': 'Asia/Bangkok',
    'country': {'code': 'VN'}, 'coordinates': {'latitude': 21.021939, 'longitude': 105.818806},
    'sensors': [{'id': 21632, 'parameter': {'id': 2, 'name': 'pm25', 'units': 'µg/m³'}}],
    'datetimeFirst': {'utc': '2016-11-09T18:00:00Z'},
    'datetimeLast': {'utc': '2025-04-09T15:00:00Z'},
    'licenses': [{'name': 'US Public Domain'}],
}
BBOX = {'north': 21.25, 'south': 20.75, 'west': 105.5, 'east': 106.0}


def location_payload(*stations):
    return json.dumps({'meta': {'found': len(stations)}, 'results': list(stations)},
                      ensure_ascii=False, indent=2).encode('utf-8')


class LocationMetadataTests(unittest.TestCase):
    def setUp(self):
        t = tempfile.TemporaryDirectory()
        self.addCleanup(t.cleanup)
        self.root = Path(t.name)

    def call_ingest(self, **extras):
        kwargs = dict(root=self.root, region_id='HANOI', location_id=7441,
                      sensor_id=21632, bbox=BBOX, api_key='FAKE_TEST_KEY',
                      transport=lambda *_: location_payload(STATION),
                      sleep=lambda _: None)
        kwargs.update(extras)
        return ingest_openaq_location(**kwargs)

    def manifests(self):
        return list(self.root.glob('data/raw/openaq/manifests/locations/*.manifest.json'))

    def test_live_metadata_raw_bytes_and_null_time_window(self):
        body = location_payload(STATION)
        calls = []
        def transport(url, headers, timeout):
            calls.append((url, headers))
            return body
        summary = self.call_ingest(transport=transport)
        self.assertTrue(summary.ok)
        self.assertEqual(summary.rows, 1)
        self.assertEqual(calls[0][0], 'https://api.openaq.org/v3/locations/7441')
        self.assertEqual(calls[0][1]['X-API-Key'], 'FAKE_TEST_KEY')
        m = json.loads(self.manifests()[0].read_text())
        self.assertEqual(m['source_product'], 'openaq_v3_locations')
        self.assertEqual(m['acquisition_mode'], 'live')
        self.assertIsNone(m['start_utc'])
        self.assertIsNone(m['end_utc'])
        self.assertEqual(m['status'], 'success')
        self.assertEqual(m['row_count'], 1)
        self.assertEqual(m['sha256'], sha256_bytes(body))
        self.assertEqual((self.root / m['file']).read_bytes(), body)
        self.assertNotIn('FAKE_TEST_KEY', json.dumps(m))

    def test_replay_list_of_ten_preserves_entire_response(self):
        others = [{'id': i} for i in range(1, 10)]
        body = location_payload(*others, STATION)
        f = self.root / 'local-page.json'
        f.write_bytes(body)
        def no_network(*_):
            raise AssertionError('no network in replay')
        summary = self.call_ingest(replay_file=f, api_key=None, transport=no_network)
        self.assertTrue(summary.ok)
        self.assertEqual(summary.rows, 10)
        m = json.loads(self.manifests()[0].read_text())
        self.assertEqual(m['row_count'], 10)  # raw list size, not 1 selected station
        self.assertEqual(m['acquisition_mode'], 'replay')
        self.assertEqual((self.root / m['file']).read_bytes(), body)

    def test_missing_location_fails_safely(self):
        summary = self.call_ingest(transport=lambda *_: location_payload({'id': 111}))
        self.assertFalse(summary.ok)
        m = json.loads(self.manifests()[0].read_text())
        self.assertEqual(m['error_type'], 'selected_location_missing_or_duplicate')
        self.assertIsNone(m['file'])
        self.assertIsNone(m['sha256'])

    def test_missing_sensor_fails_safely(self):
        data = copy.deepcopy(STATION)
        data['sensors'] = [{'id': 1, 'parameter': {'name': 'pm25'}}]
        summary = self.call_ingest(transport=lambda *_: location_payload(data))
        self.assertFalse(summary.ok)
        m = json.loads(self.manifests()[0].read_text())
        self.assertEqual(m['error_type'], 'selected_sensor_missing_or_duplicate')

    def test_non_pm25_sensor_rejected(self):
        data = copy.deepcopy(STATION)
        data['sensors'][0]['parameter']['name'] = 'pm10'
        with self.assertRaises(IngestionError):
            inspect_location_payload(location_payload(data), location_id=7441, sensor_id=21632)

    def test_missing_coordinates_and_outside_region(self):
        data = copy.deepcopy(STATION)
        data['coordinates'] = None
        summary = self.call_ingest(transport=lambda *_: location_payload(data))
        self.assertFalse(summary.ok)
        self.assertEqual(json.loads(self.manifests()[0].read_text())['error_type'], 'location_missing_coordinates')
        data2 = copy.deepcopy(STATION)
        data2['coordinates']['latitude'] = 22.0
        with self.assertRaises(IngestionError) as ctx:
            inspect_location_payload(location_payload(data2), location_id=7441, sensor_id=21632, bbox=BBOX)
        self.assertEqual(ctx.exception.code, 'location_outside_region')

    def test_http_401_manifest_and_no_secret(self):
        def unauthorized(url, headers, timeout):
            raise urllib.error.HTTPError(url, 401, 'Unauthorized', {}, io.BytesIO())
        s = self.call_ingest(transport=unauthorized)
        self.assertFalse(s.ok)
        m = json.loads(self.manifests()[0].read_text())
        self.assertEqual(m['error_type'], 'http_401')
        self.assertNotIn('FAKE_TEST_KEY', json.dumps(m))

    def test_retry_http_429(self):
        count = []
        def limited(url, headers, timeout):
            count.append(1)
            if len(count) == 1:
                raise urllib.error.HTTPError(url, 429, 'Too many requests', {}, io.BytesIO())
            return location_payload(STATION)
        s = self.call_ingest(transport=limited, max_retries=1)
        self.assertTrue(s.ok)
        self.assertEqual(len(count), 2)

    def test_raw_change_not_silently_overwritten(self):
        self.assertTrue(self.call_ingest().ok)
        altered = copy.deepcopy(STATION)
        altered['name'] = 'Hanoi New'
        s = self.call_ingest(transport=lambda *_: location_payload(altered))
        self.assertFalse(s.ok)
        m = json.loads(self.manifests()[0].read_text())
        self.assertEqual(m['status'], 'success')  # prior good manifest kept
        s2 = self.call_ingest(transport=lambda *_: location_payload(altered), overwrite=True)
        self.assertTrue(s2.ok)

    def test_bad_json_and_bad_location_id(self):
        self.assertFalse(self.call_ingest(transport=lambda *_: b'not json').ok)
        with self.assertRaises(ValueError):
            self.call_ingest(location_id=-1)

    def test_manifest_schema_supports_non_temporal_location_metadata(self):
        schema = json.loads((Path(__file__).resolve().parents[1]
                             / 'contracts/schemas/manifest.schema.json').read_text())
        self.assertIn('null', schema['properties']['start_utc']['type'])
        self.assertIn('null', schema['properties']['end_utc']['type'])


if __name__ == '__main__':
    unittest.main()
