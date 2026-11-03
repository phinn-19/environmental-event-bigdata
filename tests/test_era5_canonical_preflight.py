import importlib.util
import unittest
from datetime import datetime, timezone
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import Mock

SRC = Path(__file__).resolve().parents[1] / 'scripts/bulk/era5_canonical_preflight.py'
spec = importlib.util.spec_from_file_location('preflight', SRC)
mod = importlib.util.module_from_spec(spec)
spec.loader.exec_module(mod)


class CanonicalPreflightTests(unittest.TestCase):
    def test_month_bounds_and_adjacency(self):
        self.assertEqual(mod.month_window('2023-08')[1], mod.month_window('2023-09')[0])
        self.assertFalse(mod.intervals_overlap(mod.month_window('2023-08'), mod.month_window('2023-09')))

    def test_detect_daily_overlap(self):
        manifests = [('existing.json', {
            'source':'era5', 'source_product':'reanalysis-era5-single-levels',
            'region_id':'VN_BBOX', 'start_utc':'2023-08-05T00:00:00Z',
            'end_utc':'2023-08-06T00:00:00Z'})]
        got = mod.classify_manifests(manifests, ('2023-08', '2023-09'))
        self.assertEqual([x['month'] for x in got['overlaps']], ['2023-08'])

    def test_different_region_not_conflict(self):
        m = {'source':'era5', 'region_id':'HANOI',
             'start_utc':'2023-08-01T00:00:00Z', 'end_utc':'2023-09-01T00:00:00Z'}
        self.assertEqual(mod.classify_manifests([('foo',m)], ('2023-08',))['overlaps'], [])

    def test_read_only_mock_and_private_guard(self):
        info = lambda *args, **kwargs: SimpleNamespace(private=True)
        list_tree = lambda *args, **kwargs: []
        down = Mock(side_effect=AssertionError('must not need download for empty inventory'))
        out = mod.run_remote(bucket=mod.BUCKET, token='test-token',get_info=info,
                             list_tree=list_tree,download=down,months=mod.MONTHS)
        self.assertEqual(out['canonical_era5_objects'], 0)
        self.assertEqual(out['hf_writes'], 0)
        down.assert_not_called()
        with self.assertRaises(ValueError):
            mod.run_remote(bucket=mod.BUCKET, token='x',
                           get_info=lambda *a,**k: SimpleNamespace(private=False),
                           list_tree=list_tree,download=down, months=mod.MONTHS)

    def test_refuse_unsafe_raw(self):
        self.assertFalse(mod._safe_relative('data/raw/era5/../secrets'))
        self.assertFalse(mod._safe_relative('data\\raw\\era5\\foo'))
        self.assertTrue(mod._safe_relative('data/raw/era5/reanalysis-era5-single-levels/VN_BBOX/file.grib'))

if __name__ == '__main__':
    unittest.main()
