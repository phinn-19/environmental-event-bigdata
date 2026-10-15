"""Validate contract v1.1 and synthetic algorithm fixture examples only.

These tests do not implement/test production Spark or event algorithms.
"""
import datetime as dt
import math
import statistics
import unittest
from pathlib import Path

from src.common.validation import read_json, timestamp, validate_config, validate_fixture_set

ROOT = Path(__file__).resolve().parents[1]


def _valid_history(case, lookback_days):
    t = timestamp(case['timestamp_utc'])
    earliest = t - dt.timedelta(days=lookback_days)
    return [r['value_ugm3'] for r in case['history']
            if earliest <= timestamp(r['timestamp_utc']) < t
            and timestamp(r['timestamp_utc']).hour == t.hour
            and r['has_flags'] is False
            and r['value_ugm3'] is not None
            and type(r['value_ugm3']) in (int,float)
            and math.isfinite(r['value_ugm3'])
            and r['value_ugm3'] >= 0]


def _candidate(row, absolute_threshold, ratio):
    val, baseline = row['value_ugm3'], row['baseline_ugm3']
    return (row['has_flags'] is False and row['is_usable'] is True
            and val is not None and baseline is not None and baseline >= 0
            and val - baseline >= absolute_threshold and val >= ratio*baseline)


class ContractDecisionTests(unittest.TestCase):
    def test_algorithm_config_present(self):
        cfg = validate_config(ROOT/'configs/project.yaml')
        self.assertEqual(cfg['algorithms']['air_quality']['baseline']['lookback_days'],30)
        self.assertEqual(cfg['algorithms']['air_quality']['baseline']['min_valid_samples'],10)
        self.assertEqual(cfg['algorithms']['fire']['min_samples'],2)

    def test_curated_baseline_method_matches_config(self):
        cfg = validate_config(ROOT/'configs/project.yaml')
        registry = read_json(ROOT/'contracts/schemas/table_contracts.json')
        baseline_rows = read_json(ROOT/'fixtures/curated/air_quality_baseline.json')
        self.assertEqual(baseline_rows[0]['method'],cfg['algorithms']['air_quality']['baseline']['method'])
        self.assertEqual(validate_fixture_set(ROOT)['air_quality_baseline'],len(baseline_rows))

    def test_baseline_scenario_expected_values(self):
        cfg = validate_config(ROOT/'configs/project.yaml')
        opts = cfg['algorithms']['air_quality']['baseline']
        cases = read_json(ROOT/'fixtures/scenarios/aq_baseline_cases.json')
        self.assertTrue(cases['synthetic'])
        self.assertEqual(cases['method'],opts['method'])
        for case in cases['cases']:
            values = _valid_history(case,opts['lookback_days'])
            expected = case['expected']
            self.assertEqual(len(values),expected['sample_count'])
            usable = len(values) >= opts['min_valid_samples']
            self.assertEqual(usable,expected['is_usable'])
            self.assertEqual(statistics.median(values) if usable else None,expected['baseline_ugm3'])

    def test_anomaly_two_consecutive_example(self):
        cfg=validate_config(ROOT/'configs/project.yaml')
        opts=cfg['algorithms']['air_quality']['anomaly']
        scenario=read_json(ROOT/'fixtures/scenarios/aq_anomaly_cases.json')
        self.assertTrue(scenario['synthetic'])
        rows=scenario['records']
        self.assertTrue(_candidate(rows[0],opts['min_absolute_increase_ugm3'],opts['min_ratio']))
        self.assertTrue(_candidate(rows[1],opts['min_absolute_increase_ugm3'],opts['min_ratio']))
        self.assertEqual(timestamp(rows[1]['timestamp_utc'])-timestamp(rows[0]['timestamp_utc']),dt.timedelta(hours=1))
        self.assertFalse(_candidate(rows[2],opts['min_absolute_increase_ugm3'],opts['min_ratio']))
        self.assertEqual(scenario['expected_event_windows_utc'][0], [rows[0]['timestamp_utc'],rows[2]['timestamp_utc']])

    def test_fire_fixture_membership(self):
        cfg=validate_config(ROOT/'configs/project.yaml')
        opts=cfg['algorithms']['fire']
        case=read_json(ROOT/'fixtures/scenarios/fire_cluster_cases.json')
        self.assertTrue(case['synthetic'])
        rows={r['hotspot_id']:r for r in case['observations']}
        self.assertEqual(len(set(rows)),len(case['observations']))
        a,b=[rows[k] for k in case['expected_cluster_member_ids']]
        self.assertIn(a['confidence_raw'],opts['accepted_confidence'])
        self.assertIn(b['confidence_raw'],opts['accepted_confidence'])
        self.assertLessEqual(abs(timestamp(a['timestamp_utc'])-timestamp(b['timestamp_utc'])),dt.timedelta(hours=opts['temporal_radius_hours']))
        def haversine(x,y):
            p1,p2=map(math.radians,[x['latitude'],y['latitude']]);dl=math.radians(y['longitude']-x['longitude'])
            return 6371*2*math.asin(math.sqrt(math.sin((p2-p1)/2)**2+math.cos(p1)*math.cos(p2)*math.sin(dl/2)**2))
        self.assertLessEqual(haversine(a,b),opts['spatial_radius_km'])
        far=rows['synthetic-fire-c'];low=rows['synthetic-fire-low'];late=rows['synthetic-fire-late']
        self.assertGreater(haversine(a,far),opts['spatial_radius_km'])
        self.assertNotIn(low['confidence_raw'],opts['accepted_confidence'])
        self.assertGreater(abs(timestamp(a['timestamp_utc'])-timestamp(late['timestamp_utc'])),dt.timedelta(hours=opts['temporal_radius_hours']))


if __name__=='__main__':
    unittest.main()
