"""Foundation tests. Only stdlib + PyYAML; no Spark, no secrets, no API calls."""
import copy
import json
import tempfile
import unittest
from pathlib import Path

from src.common.validation import (
    ContractError, read_json, validate_config, validate_events,
    validate_fixture_set, validate_table_rows,
)

ROOT = Path(__file__).resolve().parents[1]


class FoundationTests(unittest.TestCase):
    def test_public_config(self):
        cfg = validate_config(ROOT / 'configs/project.yaml')
        self.assertIsNone(cfg['integration']['start_utc'])
        self.assertEqual(cfg['processing']['spark_master'], 'local[2]')

    def test_fixture_tables_and_events(self):
        counts = validate_fixture_set(ROOT)
        self.assertEqual(len(counts), 7)
        self.assertGreaterEqual(counts['events'], 2)

    def test_events_empty_array_supported(self):
        self.assertEqual(validate_events([]), 0)

    def test_detect_bad_event(self):
        ev = read_json(ROOT/'fixtures/events/events_mock.json')
        ev[0]['start_time_utc']='2024-11-01T10:00:00'  # lacks UTC Z
        with self.assertRaises(ContractError): validate_events(ev)

    def test_null_and_flags_retained(self):
        rows = read_json(ROOT/'fixtures/silver/air_quality_hourly.json')
        registry = read_json(ROOT/'contracts/schemas/table_contracts.json')
        self.assertEqual(validate_table_rows('air_quality_hourly',rows,registry),2)
        self.assertIsNone(rows[0]['value_ugm3'])
        self.assertTrue(rows[0]['has_flags'])

    def test_detect_duplicate_keys(self):
        rows = read_json(ROOT/'fixtures/silver/air_quality_hourly.json')
        registry = read_json(ROOT/'contracts/schemas/table_contracts.json')
        with self.assertRaises(ContractError):
            validate_table_rows('air_quality_hourly',rows+[copy.deepcopy(rows[0])],registry)

    def test_invalid_integration_window(self):
        raw=(ROOT/'configs/project.yaml').read_text(encoding='utf-8')
        with tempfile.TemporaryDirectory() as tmp:
            p=Path(tmp)/'bad.yaml'
            p.write_text(raw.replace('start_utc: null','start_utc: "2024-11-01T00:00:00Z"'),encoding='utf-8')
            with self.assertRaises(ContractError): validate_config(p)

    def test_json_no_nan(self):
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp)/'bad.json'
            path.write_text('[NaN]', encoding='utf-8')
            with self.assertRaises(ContractError): read_json(path)


if __name__=='__main__':
    unittest.main()
